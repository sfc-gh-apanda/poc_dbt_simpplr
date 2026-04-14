-- ═══════════════════════════════════════════════════════════════════════════════
-- PUBLISH + ARCHIVE — Stored Procedures Setup
-- Run ONCE after audit_setup.sql
--
-- PRC_MERGE_PUBLISH:
--   Dynamic MERGE helper — builds MERGE SQL from INFORMATION_SCHEMA metadata.
--   Used for INTERACTION and CATEGORY (no _HIST table, delta MERGE into DBT_UDL).
--   Stamps PUBLISHED_BY_RUN_ID and PUBLISHED_AT on target after MERGE.
--
-- PRC_DBT_PUBLISH_TO_TARGET(P_RUN_ID, P_BATCH_RUN_ID, P_FULL_LOAD_ENTITY):
--   Dynamic, entity-driven publish from DBT_UDL_BATCH_PROCESS → DBT_UDL.
--   Features:
--     - Entity config via cursor (extensible without code changes)
--     - Dynamic column discovery from INFORMATION_SCHEMA.COLUMNS
--     - Full load support: P_FULL_LOAD_ENTITY = 'NONE'|'ALL'|'NEWSLETTER,...'
--     - Step-level logging per entity (RUNNING → SUCCESS / ERROR / SKIPPED)
--   Strategies:
--     NEWSLETTER (HIST_MASTER): deactivate/delete HIST → insert → derive target
--     INTERACTION / CATEGORY (MERGE): delta MERGE or TRUNCATE+INSERT on full load
--   P_RUN_ID = dbt invocation_id, P_BATCH_RUN_ID = Airflow batch_run_id
--
-- PRC_DBT_ARCHIVE_RAW_DATA:
--   Archive processed raw records and purge from source tables.
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;
USE DATABASE COMMON_TENANT_DEV;

-- ═══════════════════════════════════════════════════════════════════════════════
-- 1. PRC_MERGE_PUBLISH
--    Dynamic MERGE helper: builds column lists from INFORMATION_SCHEMA metadata.
--    Designed to be called within the caller's transaction — no BEGIN/COMMIT.
--    Merge key: (TENANT_CODE, CODE).  Skip condition: HASH_VALUE unchanged.
--    Delta-aware: no WHEN NOT MATCHED BY SOURCE (wrk has delta only, not full state).
--    Stamps PUBLISHED_BY_RUN_ID and PUBLISHED_AT on target after MERGE.
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE DBT_UDL_BATCH_PROCESS.PRC_MERGE_PUBLISH(
    P_SOURCE_SCHEMA      VARCHAR,
    P_SOURCE_TABLE       VARCHAR,
    P_TARGET_SCHEMA      VARCHAR,
    P_TARGET_TABLE       VARCHAR,
    P_PUBLISHED_BY_RUN_ID VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_all_cols   VARCHAR;
    v_src_cols   VARCHAR;
    v_update_set VARCHAR;
    v_merge_sql  VARCHAR;
BEGIN

    SELECT LISTAGG(COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
    INTO v_all_cols
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = :P_SOURCE_SCHEMA AND TABLE_NAME = :P_SOURCE_TABLE;

    SELECT LISTAGG('src.' || COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
    INTO v_src_cols
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = :P_SOURCE_SCHEMA AND TABLE_NAME = :P_SOURCE_TABLE;

    SELECT LISTAGG('tgt.' || COLUMN_NAME || ' = src.' || COLUMN_NAME, ', ')
           WITHIN GROUP (ORDER BY ORDINAL_POSITION)
    INTO v_update_set
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = :P_SOURCE_SCHEMA AND TABLE_NAME = :P_SOURCE_TABLE
      AND COLUMN_NAME NOT IN ('TENANT_CODE', 'CODE');

    v_merge_sql :=
        'MERGE INTO ' || :P_TARGET_SCHEMA || '.' || :P_TARGET_TABLE || ' tgt ' ||
        'USING ' || :P_SOURCE_SCHEMA || '.' || :P_SOURCE_TABLE || ' src ' ||
        'ON tgt.TENANT_CODE = src.TENANT_CODE AND tgt.CODE = src.CODE ' ||
        'WHEN MATCHED THEN UPDATE SET ' || :v_update_set || ' ' ||
        'WHEN NOT MATCHED THEN INSERT (' || :v_all_cols || ') VALUES (' || :v_src_cols || ')';

    EXECUTE IMMEDIATE :v_merge_sql;

    IF (:P_PUBLISHED_BY_RUN_ID IS NOT NULL) THEN
        EXECUTE IMMEDIATE
            'UPDATE ' || :P_TARGET_SCHEMA || '.' || :P_TARGET_TABLE || ' tgt ' ||
            'SET tgt.PUBLISHED_BY_RUN_ID = ''' || :P_PUBLISHED_BY_RUN_ID || ''',' ||
            '    tgt.PUBLISHED_AT = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ ' ||
            'FROM ' || :P_SOURCE_SCHEMA || '.' || :P_SOURCE_TABLE || ' src ' ||
            'WHERE tgt.TENANT_CODE = src.TENANT_CODE AND tgt.CODE = src.CODE';
    END IF;

    RETURN 'SUCCESS';
END;
$$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 2. PRC_DBT_PUBLISH_TO_TARGET
--    Dynamic, entity-driven publish with full load support and step-level logging.
--
--    Entity configuration (cursor-based, extensible):
--      NEWSLETTER:             HIST-as-master SCD-2 (wrk→HIST→derived NEWSLETTER)
--      NEWSLETTER_INTERACTION: Direct MERGE (delta) or TRUNCATE+INSERT (full load)
--      NEWSLETTER_CATEGORY:    Direct MERGE (delta) or TRUNCATE+INSERT (full load)
--
--    Column lists are built dynamically from INFORMATION_SCHEMA.COLUMNS.
--    Step-level audit logs (RUNNING → SUCCESS/ERROR/SKIPPED) per entity in DBT_MODEL_LOG.
--
--    P_RUN_ID            = dbt invocation_id
--    P_BATCH_RUN_ID      = Airflow batch_run_id
--    P_FULL_LOAD_ENTITY  = 'NONE' | 'ALL' | comma-separated entity names
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE DBT_UDL_BATCH_PROCESS.PRC_DBT_PUBLISH_TO_TARGET(
    P_RUN_ID            VARCHAR,
    P_BATCH_RUN_ID      INTEGER DEFAULT 0,
    P_FULL_LOAD_ENTITY  VARCHAR DEFAULT 'NONE'
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_full_load_upper   VARCHAR;
    v_entity_key        VARCHAR;
    v_wrk_table         VARCHAR;
    v_target_table      VARCHAR;
    v_hist_table        VARCHAR;
    v_publish_type      VARCHAR;
    v_is_entity_full    BOOLEAN;
    v_target_cols       VARCHAR;
    v_select_cols       VARCHAR;
    v_src_count         INTEGER;
    v_deactivated       INTEGER DEFAULT 0;
    v_inserted          INTEGER DEFAULT 0;
    v_step_log_id       VARCHAR;
    v_full_load_check   VARCHAR;

    v_nl_deactivated    INTEGER DEFAULT 0;
    v_nl_inserted       INTEGER DEFAULT 0;
    v_nli_inserted      INTEGER DEFAULT 0;
    v_nlc_inserted      INTEGER DEFAULT 0;
    v_nl_count          INTEGER DEFAULT 0;
    v_nli_count         INTEGER DEFAULT 0;
    v_nlc_count         INTEGER DEFAULT 0;

    c_entities CURSOR FOR
        SELECT column1, column2, column3, column4, column5
        FROM VALUES
            ('NEWSLETTER',             'WRK_NEWSLETTER',             'NEWSLETTER',             'NEWSLETTER_HIST', 'HIST_MASTER'),
            ('NEWSLETTER_INTERACTION', 'WRK_NEWSLETTER_INTERACTION', 'NEWSLETTER_INTERACTION', NULL,              'MERGE'),
            ('NEWSLETTER_CATEGORY',    'WRK_NEWSLETTER_CATEGORY',    'NEWSLETTER_CATEGORY',    NULL,              'MERGE');

BEGIN
    v_full_load_upper := UPPER(COALESCE(:P_FULL_LOAD_ENTITY, 'NONE'));

    SELECT COUNT(*) INTO v_nl_count  FROM DBT_UDL_BATCH_PROCESS.WRK_NEWSLETTER;
    SELECT COUNT(*) INTO v_nli_count FROM DBT_UDL_BATCH_PROCESS.WRK_NEWSLETTER_INTERACTION;
    SELECT COUNT(*) INTO v_nlc_count FROM DBT_UDL_BATCH_PROCESS.WRK_NEWSLETTER_CATEGORY;

    -- ── Step-level logging: INITIATE all entities (outside transaction — survives rollback) ──
    MERGE INTO DBT_EXECUTION_RUN_STATS.DBT_MODEL_LOG tgt
    USING (
        SELECT :P_RUN_ID || '_publish_NEWSLETTER'             AS log_id, 'publish_NEWSLETTER'             AS model_name, :v_nl_count  AS src_count
        UNION ALL
        SELECT :P_RUN_ID || '_publish_NEWSLETTER_INTERACTION',            'publish_NEWSLETTER_INTERACTION',            :v_nli_count
        UNION ALL
        SELECT :P_RUN_ID || '_publish_NEWSLETTER_CATEGORY',              'publish_NEWSLETTER_CATEGORY',              :v_nlc_count
    ) src ON tgt.log_id = src.log_id
    WHEN NOT MATCHED THEN INSERT (
        log_id, run_id, project_name, model_name, schema_name,
        database_name, materialization, batch_id, status, started_at, rows_affected, is_incremental
    ) VALUES (
        src.log_id, :P_RUN_ID, 'newsletter_poc', src.model_name, 'DBT_UDL',
        CURRENT_DATABASE(), 'publish', :P_RUN_ID || '_publish', 'RUNNING',
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, src.src_count, FALSE
    );

    BEGIN TRANSACTION;

    -- ── Dynamic entity processing loop ──
    FOR entity IN c_entities DO
        v_entity_key    := entity.column1;
        v_wrk_table     := entity.column2;
        v_target_table  := entity.column3;
        v_hist_table    := entity.column4;
        v_publish_type  := entity.column5;
        v_deactivated   := 0;
        v_inserted      := 0;

        v_full_load_check := ',' || v_full_load_upper || ',';
        v_is_entity_full := (v_full_load_upper = 'ALL'
                             OR CONTAINS(v_full_load_check, ',' || v_entity_key || ','));

        IF (v_publish_type = 'HIST_MASTER') THEN
            -- ═══ NEWSLETTER: HIST-as-master SCD-2 ═══

            -- Dynamic column list for HIST table
            SELECT LISTAGG(COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
            INTO v_target_cols
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'DBT_UDL' AND TABLE_NAME = :v_hist_table;

            -- Build SELECT mapping: WRK columns + publish audit overrides
            SELECT LISTAGG(
                CASE
                    WHEN COLUMN_NAME = 'PUBLISHED_BY_RUN_ID' THEN '''' || :P_RUN_ID || ''''
                    WHEN COLUMN_NAME = 'PUBLISHED_AT' THEN 'CURRENT_TIMESTAMP()::TIMESTAMP_NTZ'
                    ELSE 'w.' || COLUMN_NAME
                END, ', '
            ) WITHIN GROUP (ORDER BY ORDINAL_POSITION)
            INTO v_select_cols
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'DBT_UDL' AND TABLE_NAME = :v_hist_table;

            IF (v_is_entity_full) THEN
                -- FULL LOAD: purge all from HIST (preserve sentinel id = -1)
                EXECUTE IMMEDIATE 'DELETE FROM DBT_UDL.' || :v_hist_table || ' WHERE id != -1';
                v_deactivated := SQLROWCOUNT;
            ELSE
                -- DELTA: deactivate superseded records in HIST
                EXECUTE IMMEDIATE
                    'UPDATE DBT_UDL.' || :v_hist_table || ' h ' ||
                    'SET h.ACTIVE_FLAG = FALSE, ' ||
                    '    h.INACTIVE_DATE = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, ' ||
                    '    h.UPDATED_BY = CURRENT_USER(), ' ||
                    '    h.UPDATED_DATETIME = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ ' ||
                    'WHERE h.ACTIVE_FLAG = TRUE ' ||
                    '  AND EXISTS (SELECT 1 FROM DBT_UDL_BATCH_PROCESS.' || :v_wrk_table || ' w ' ||
                    '              WHERE w.TENANT_CODE = h.TENANT_CODE AND w.CODE = h.CODE)';
                v_deactivated := SQLROWCOUNT;
            END IF;

            -- Insert new records from WRK into HIST (dynamic columns)
            EXECUTE IMMEDIATE
                'INSERT INTO DBT_UDL.' || :v_hist_table || ' (' || v_target_cols || ') ' ||
                'SELECT ' || v_select_cols || ' FROM DBT_UDL_BATCH_PROCESS.' || :v_wrk_table || ' w';
            v_inserted := SQLROWCOUNT;

            -- Derive target table from active HIST records (dynamic columns)
            SELECT LISTAGG(COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
            INTO v_target_cols
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'DBT_UDL' AND TABLE_NAME = :v_target_table;

            EXECUTE IMMEDIATE 'TRUNCATE TABLE DBT_UDL.' || :v_target_table;
            EXECUTE IMMEDIATE
                'INSERT INTO DBT_UDL.' || :v_target_table || ' (' || v_target_cols || ') ' ||
                'SELECT ' || v_target_cols || ' FROM DBT_UDL.' || :v_hist_table || ' WHERE ACTIVE_FLAG = TRUE';

            v_nl_deactivated := v_deactivated;
            v_nl_inserted    := v_inserted;

        ELSE
            -- ═══ INTERACTION / CATEGORY: direct publish ═══

            IF (v_is_entity_full) THEN
                -- FULL LOAD: truncate + dynamic insert
                SELECT LISTAGG(COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION)
                INTO v_target_cols
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'DBT_UDL' AND TABLE_NAME = :v_target_table;

                SELECT LISTAGG(
                    CASE
                        WHEN COLUMN_NAME = 'PUBLISHED_BY_RUN_ID' THEN '''' || :P_RUN_ID || ''''
                        WHEN COLUMN_NAME = 'PUBLISHED_AT' THEN 'CURRENT_TIMESTAMP()::TIMESTAMP_NTZ'
                        ELSE 'w.' || COLUMN_NAME
                    END, ', '
                ) WITHIN GROUP (ORDER BY ORDINAL_POSITION)
                INTO v_select_cols
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'DBT_UDL' AND TABLE_NAME = :v_target_table;

                EXECUTE IMMEDIATE 'TRUNCATE TABLE DBT_UDL.' || :v_target_table;
                EXECUTE IMMEDIATE
                    'INSERT INTO DBT_UDL.' || :v_target_table || ' (' || v_target_cols || ') ' ||
                    'SELECT ' || v_select_cols || ' FROM DBT_UDL_BATCH_PROCESS.' || :v_wrk_table || ' w';
                v_inserted := SQLROWCOUNT;
            ELSE
                -- DELTA: MERGE via helper procedure
                CALL DBT_UDL_BATCH_PROCESS.PRC_MERGE_PUBLISH(
                    'DBT_UDL_BATCH_PROCESS', :v_wrk_table, 'DBT_UDL', :v_target_table, :P_RUN_ID
                );
                -- Use pre-counted source rows as merge count
                IF (v_entity_key = 'NEWSLETTER_INTERACTION') THEN
                    v_inserted := v_nli_count;
                ELSE
                    v_inserted := v_nlc_count;
                END IF;
            END IF;

            IF (v_entity_key = 'NEWSLETTER_INTERACTION') THEN
                v_nli_inserted := v_inserted;
            ELSE
                v_nlc_inserted := v_inserted;
            END IF;

        END IF;

    END FOR;

    COMMIT;

    -- ── Update step logs to SUCCESS with per-entity row counts ──
    UPDATE DBT_EXECUTION_RUN_STATS.DBT_MODEL_LOG
    SET status        = 'SUCCESS',
        ended_at      = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
        rows_affected = :v_nl_inserted + :v_nl_deactivated,
        materialization = CASE
            WHEN :v_full_load_upper = 'ALL' OR CONTAINS(',' || :v_full_load_upper || ',', ',NEWSLETTER,')
            THEN 'full_load_hist' ELSE 'hist_as_master' END
    WHERE log_id = :P_RUN_ID || '_publish_NEWSLETTER';

    UPDATE DBT_EXECUTION_RUN_STATS.DBT_MODEL_LOG
    SET status        = 'SUCCESS',
        ended_at      = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
        rows_affected = :v_nli_inserted,
        materialization = CASE
            WHEN :v_full_load_upper = 'ALL' OR CONTAINS(',' || :v_full_load_upper || ',', ',NEWSLETTER_INTERACTION,')
            THEN 'full_load' ELSE 'merge' END
    WHERE log_id = :P_RUN_ID || '_publish_NEWSLETTER_INTERACTION';

    UPDATE DBT_EXECUTION_RUN_STATS.DBT_MODEL_LOG
    SET status        = 'SUCCESS',
        ended_at      = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
        rows_affected = :v_nlc_inserted,
        materialization = CASE
            WHEN :v_full_load_upper = 'ALL' OR CONTAINS(',' || :v_full_load_upper || ',', ',NEWSLETTER_CATEGORY,')
            THEN 'full_load' ELSE 'merge' END
    WHERE log_id = :P_RUN_ID || '_publish_NEWSLETTER_CATEGORY';

    -- Mark reprocess requests as completed
    UPDATE DBT_UDL_BATCH_PROCESS.REPROCESS_REQUEST
    SET    STATUS              = 'COMPLETED',
           PROCESSED_BY_RUN_ID = :P_RUN_ID,
           PROCESSED_AT        = CURRENT_TIMESTAMP(),
           BATCH_RUN_ID        = :P_BATCH_RUN_ID
    WHERE  STATUS = 'PENDING';

    INSERT INTO DBT_UDL_BATCH_PROCESS.REPROCESS_REQUEST_HIST
    SELECT * FROM DBT_UDL_BATCH_PROCESS.REPROCESS_REQUEST
    WHERE STATUS = 'COMPLETED';

    DELETE FROM DBT_UDL_BATCH_PROCESS.REPROCESS_REQUEST
    WHERE STATUS = 'COMPLETED';

    RETURN OBJECT_CONSTRUCT(
        'status',             'success',
        'run_id',             P_RUN_ID,
        'batch_run_id',       P_BATCH_RUN_ID,
        'full_load_entity',   P_FULL_LOAD_ENTITY,
        'strategy',           'dynamic_publish',
        'nl_delta',           v_nl_count,
        'nl_deactivated',     v_nl_deactivated,
        'nl_hist_inserted',   v_nl_inserted,
        'interaction',        v_nli_count,
        'category',           v_nlc_count
    )::VARCHAR;

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        -- Mark the failed entity as ERROR (INITIATE logs survive since they were outside the transaction)
        UPDATE DBT_EXECUTION_RUN_STATS.DBT_MODEL_LOG
        SET status        = 'ERROR',
            ended_at      = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
            error_message = SQLERRM
        WHERE log_id = :P_RUN_ID || '_publish_' || :v_entity_key
          AND status = 'RUNNING';

        -- Mark remaining un-processed entities as SKIPPED
        UPDATE DBT_EXECUTION_RUN_STATS.DBT_MODEL_LOG
        SET status   = 'SKIPPED',
            ended_at = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
        WHERE run_id = :P_RUN_ID
          AND model_name LIKE 'publish_%'
          AND status = 'RUNNING';

        RETURN OBJECT_CONSTRUCT(
            'status',           'failed',
            'run_id',           P_RUN_ID,
            'batch_run_id',     P_BATCH_RUN_ID,
            'full_load_entity', P_FULL_LOAD_ENTITY,
            'failed_entity',    v_entity_key,
            'error',            SQLERRM
        )::VARCHAR;
END;
$$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 2. PRC_DBT_ARCHIVE_RAW_DATA
--    Archive processed raw records, then purge from source tables.
--    Uses data_process_end_time as the boundary — matches staging filters.
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE DBT_UDL_BATCH_PROCESS.PRC_DBT_ARCHIVE_RAW_DATA(
    P_RUN_ID   VARCHAR,
    P_END_TIME VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_nl_archived   INTEGER DEFAULT 0;
    v_nli_archived  INTEGER DEFAULT 0;
    v_nlc_archived  INTEGER DEFAULT 0;
BEGIN

    BEGIN TRANSACTION;

    -- ═══════════════════════════════════════════════════════════
    -- NEWSLETTER: archive + purge
    -- ═══════════════════════════════════════════════════════════
    INSERT INTO SHARED_SERVICES_STAGING.ENL_NEWSLETTER_ARCHIVE
    SELECT *
    FROM SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER
    WHERE CREATED_DATETIME <= :P_END_TIME::TIMESTAMP_NTZ;

    v_nl_archived := SQLROWCOUNT;

    DELETE FROM SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER
    WHERE CREATED_DATETIME <= :P_END_TIME::TIMESTAMP_NTZ;

    -- ═══════════════════════════════════════════════════════════
    -- NEWSLETTER INTERACTION: archive + purge
    -- ═══════════════════════════════════════════════════════════
    INSERT INTO SHARED_SERVICES_STAGING.ENL_NEWSLETTER_INTERACTION_ARCHIVE
    SELECT *
    FROM SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_INTERACTION
    WHERE CREATED_DATETIME <= :P_END_TIME::TIMESTAMP_NTZ;

    v_nli_archived := SQLROWCOUNT;

    DELETE FROM SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_INTERACTION
    WHERE CREATED_DATETIME <= :P_END_TIME::TIMESTAMP_NTZ;

    -- ═══════════════════════════════════════════════════════════
    -- NEWSLETTER CATEGORY: archive + purge
    -- ═══════════════════════════════════════════════════════════
    INSERT INTO SHARED_SERVICES_STAGING.ENL_NEWSLETTER_CATEGORY_ARCHIVE
    SELECT *
    FROM SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_CATEGORY
    WHERE CREATED_DATETIME <= :P_END_TIME::TIMESTAMP_NTZ;

    v_nlc_archived := SQLROWCOUNT;

    DELETE FROM SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_CATEGORY
    WHERE CREATED_DATETIME <= :P_END_TIME::TIMESTAMP_NTZ;

    COMMIT;

    -- Log the archive event
    INSERT INTO DBT_EXECUTION_RUN_STATS.DBT_MODEL_LOG (
        log_id, run_id, project_name, model_name,
        schema_name, database_name, materialization,
        batch_id, status, started_at, ended_at,
        rows_affected, is_incremental
    )
    SELECT
        :P_RUN_ID || '_archive_raw_data',
        :P_RUN_ID,
        'newsletter_poc',
        'archive_raw_data',
        'SHARED_SERVICES_STAGING',
        CURRENT_DATABASE(),
        'archive',
        :P_RUN_ID || '_archive',
        'SUCCESS',
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        :v_nl_archived + :v_nli_archived + :v_nlc_archived,
        FALSE
    WHERE NOT EXISTS (
        SELECT 1 FROM DBT_EXECUTION_RUN_STATS.DBT_MODEL_LOG
        WHERE log_id = :P_RUN_ID || '_archive_raw_data'
    );

    RETURN OBJECT_CONSTRUCT(
        'status',                   'success',
        'run_id',                   P_RUN_ID,
        'newsletter_archived',      v_nl_archived,
        'interaction_archived',     v_nli_archived,
        'category_archived',        v_nlc_archived
    )::VARCHAR;

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN OBJECT_CONSTRUCT(
            'status', 'failed',
            'run_id', P_RUN_ID,
            'error',  SQLERRM
        )::VARCHAR;
END;
$$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 3. Safety net: ensure NEWSLETTER_HIST has all required columns
--    The DDL in account_bootstrap.sql creates these, but ALTER IF NOT EXISTS
--    guards against partial setups.
-- ═══════════════════════════════════════════════════════════════════════════════

ALTER TABLE DBT_UDL.NEWSLETTER_HIST ADD COLUMN IF NOT EXISTS
    dbt_loaded_at TIMESTAMP_NTZ;

ALTER TABLE DBT_UDL.NEWSLETTER_HIST ADD COLUMN IF NOT EXISTS
    dbt_run_id VARCHAR(50);

ALTER TABLE DBT_UDL.NEWSLETTER_HIST ADD COLUMN IF NOT EXISTS
    dbt_batch_id VARCHAR(32);

ALTER TABLE DBT_UDL.NEWSLETTER_HIST ADD COLUMN IF NOT EXISTS
    dbt_source_model VARCHAR(100);

ALTER TABLE DBT_UDL.NEWSLETTER_HIST ADD COLUMN IF NOT EXISTS
    dbt_environment VARCHAR(20);

ALTER TABLE DBT_UDL.NEWSLETTER_HIST ADD COLUMN IF NOT EXISTS
    published_by_run_id VARCHAR(100);

ALTER TABLE DBT_UDL.NEWSLETTER_HIST ADD COLUMN IF NOT EXISTS
    published_at TIMESTAMP_NTZ;
