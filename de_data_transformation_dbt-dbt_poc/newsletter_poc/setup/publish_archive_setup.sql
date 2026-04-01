-- ═══════════════════════════════════════════════════════════════════════════════
-- PUBLISH + ARCHIVE — Stored Procedures Setup
-- Run ONCE after audit_setup.sql
--
-- PRC_MERGE_PUBLISH:
--   Dynamic MERGE helper — builds MERGE SQL from INFORMATION_SCHEMA metadata.
--   Only touches changed rows (hash-based), preserves Time Travel on UDL tables.
--
-- PRC_DBT_PUBLISH_TO_TARGET:
--   Hybrid Merge+Clone from DBT_UDL → UDL:
--     MERGE for 3 fact tables (preserves Time Travel)
--     CLONE for SCD2 (history table — Time Travel is redundant)
--     INSERT for NEWSLETTER_HIST (append-only accumulator)
--   All within a single transaction for atomicity.
--
-- PRC_DBT_ARCHIVE_RAW_DATA:
--   Archive processed raw records and purge from source tables.
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;
USE DATABASE COMMON_TENANT_DEV;

-- ═══════════════════════════════════════════════════════════════════════════════
-- 1. PRC_MERGE_PUBLISH
--    Dynamic MERGE helper: builds column lists from INFORMATION_SCHEMA.
--    Designed to be called within the caller's transaction — no BEGIN/COMMIT.
--    Merge key: (TENANT_CODE, CODE).  Skip condition: HASH_VALUE unchanged.
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE UDL_BATCH_PROCESS.PRC_MERGE_PUBLISH(
    P_SOURCE_SCHEMA VARCHAR,
    P_SOURCE_TABLE  VARCHAR,
    P_TARGET_SCHEMA VARCHAR,
    P_TARGET_TABLE  VARCHAR
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
        'WHEN MATCHED AND src.HASH_VALUE != tgt.HASH_VALUE THEN UPDATE SET ' || :v_update_set || ' ' ||
        'WHEN NOT MATCHED THEN INSERT (' || :v_all_cols || ') VALUES (' || :v_src_cols || ') ' ||
        'WHEN NOT MATCHED BY SOURCE THEN DELETE';

    EXECUTE IMMEDIATE :v_merge_sql;

    RETURN 'SUCCESS';
END;
$$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 2. PRC_DBT_PUBLISH_TO_TARGET
--    Hybrid publish: MERGE fact tables + CLONE SCD2 + INSERT history
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE UDL_BATCH_PROCESS.PRC_DBT_PUBLISH_TO_TARGET(
    P_RUN_ID VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_nl_count  INTEGER;
    v_nli_count INTEGER;
    v_nlc_count INTEGER;
BEGIN

    SELECT COUNT(*) INTO v_nl_count  FROM DBT_UDL.WRK_NEWSLETTER;
    SELECT COUNT(*) INTO v_nli_count FROM DBT_UDL.WRK_NEWSLETTER_INTERACTION;
    SELECT COUNT(*) INTO v_nlc_count FROM DBT_UDL.WRK_NEWSLETTER_CATEGORY;

    BEGIN TRANSACTION;

    -- ─── MERGE: 3 fact tables (preserves Time Travel on UDL targets) ───
    CALL UDL_BATCH_PROCESS.PRC_MERGE_PUBLISH(
        'DBT_UDL', 'WRK_NEWSLETTER', 'UDL', 'NEWSLETTER'
    );
    CALL UDL_BATCH_PROCESS.PRC_MERGE_PUBLISH(
        'DBT_UDL', 'WRK_NEWSLETTER_INTERACTION', 'UDL', 'NEWSLETTER_INTERACTION'
    );
    CALL UDL_BATCH_PROCESS.PRC_MERGE_PUBLISH(
        'DBT_UDL', 'WRK_NEWSLETTER_CATEGORY', 'UDL', 'NEWSLETTER_CATEGORY'
    );

    -- ─── CLONE: SCD2 (Time Travel redundant — table IS the history) ───
    CREATE OR REPLACE TABLE UDL.NEWSLETTER_SCD2
        CLONE DBT_UDL.SNAP_NEWSLETTER
        COPY GRANTS;

    -- ─── INSERT: append current newsletter state to history table ───
    INSERT INTO UDL.NEWSLETTER_HIST
    SELECT
        n.*,
        :P_RUN_ID   AS published_by_run_id,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS published_at
    FROM UDL.NEWSLETTER n;

    COMMIT;

    -- Log the publish event
    INSERT INTO DBT_EXECUTION_RUN_STATS.DBT_MODEL_LOG (
        log_id, run_id, project_name, model_name,
        schema_name, database_name, materialization,
        batch_id, status, started_at, ended_at,
        rows_affected, is_incremental
    )
    SELECT
        :P_RUN_ID || '_publish_to_target',
        :P_RUN_ID,
        'newsletter_poc',
        'publish_to_target',
        'UDL',
        CURRENT_DATABASE(),
        'merge_clone',
        :P_RUN_ID || '_publish',
        'SUCCESS',
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP(),
        :v_nl_count + :v_nli_count + :v_nlc_count,
        FALSE
    WHERE NOT EXISTS (
        SELECT 1 FROM DBT_EXECUTION_RUN_STATS.DBT_MODEL_LOG
        WHERE log_id = :P_RUN_ID || '_publish_to_target'
    );

    RETURN OBJECT_CONSTRUCT(
        'status',      'success',
        'run_id',      P_RUN_ID,
        'strategy',    'hybrid_merge_clone',
        'newsletter',  v_nl_count,
        'interaction', v_nli_count,
        'category',    v_nlc_count
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
-- 2. PRC_DBT_ARCHIVE_RAW_DATA
--    Archive processed raw records, then purge from source tables.
--    Uses data_process_end_time as the boundary — matches staging filters.
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE UDL_BATCH_PROCESS.PRC_DBT_ARCHIVE_RAW_DATA(
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
-- 3. Add dbt audit columns + publish tracking columns to NEWSLETTER_HIST
--    These columns are present on WRK_NEWSLETTER (and therefore on
--    UDL.NEWSLETTER after CLONE). The INSERT * requires HIST to match.
-- ═══════════════════════════════════════════════════════════════════════════════

ALTER TABLE UDL.NEWSLETTER_HIST ADD COLUMN IF NOT EXISTS
    dbt_loaded_at TIMESTAMP_NTZ;

ALTER TABLE UDL.NEWSLETTER_HIST ADD COLUMN IF NOT EXISTS
    dbt_run_id VARCHAR(50);

ALTER TABLE UDL.NEWSLETTER_HIST ADD COLUMN IF NOT EXISTS
    dbt_batch_id VARCHAR(32);

ALTER TABLE UDL.NEWSLETTER_HIST ADD COLUMN IF NOT EXISTS
    dbt_source_model VARCHAR(100);

ALTER TABLE UDL.NEWSLETTER_HIST ADD COLUMN IF NOT EXISTS
    dbt_environment VARCHAR(20);

ALTER TABLE UDL.NEWSLETTER_HIST ADD COLUMN IF NOT EXISTS
    published_by_run_id VARCHAR(100);

ALTER TABLE UDL.NEWSLETTER_HIST ADD COLUMN IF NOT EXISTS
    published_at TIMESTAMP_NTZ;
