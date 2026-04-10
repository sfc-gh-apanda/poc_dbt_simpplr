-- ═══════════════════════════════════════════════════════════════════════════════
-- PUBLISH + ARCHIVE — Stored Procedures Setup
-- Run ONCE after audit_setup.sql
--
-- PRC_MERGE_PUBLISH:
--   Dynamic MERGE helper — builds MERGE SQL from INFORMATION_SCHEMA metadata.
--   Used for INTERACTION and CATEGORY (no _HIST table, delta MERGE into UDL).
--   Stamps PUBLISHED_BY_RUN_ID and PUBLISHED_AT on target after MERGE.
--
-- PRC_DBT_PUBLISH_TO_TARGET(P_RUN_ID, P_BATCH_RUN_ID):
--   HIST-as-master publish from DBT_UDL → UDL:
--     1. NEWSLETTER: deactivate superseded in NEWSLETTER_HIST, insert new active
--        rows, then TRUNCATE+INSERT UDL.NEWSLETTER from active HIST records
--     2. INTERACTION / CATEGORY: delta MERGE + stamp published_by_run_id
--   All within a single transaction for atomicity.
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

CREATE OR REPLACE PROCEDURE UDL_BATCH_PROCESS.PRC_MERGE_PUBLISH(
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
--    HIST-as-master publish:
--      NEWSLETTER: deactivate in HIST → insert active → TRUNCATE+INSERT UDL.NEWSLETTER
--      INTERACTION / CATEGORY: delta MERGE into UDL (preserves Time Travel)
--    Accepts P_RUN_ID (dbt invocation_id) and P_BATCH_RUN_ID (Airflow batch_run_id)
--    for end-to-end traceability.
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE UDL_BATCH_PROCESS.PRC_DBT_PUBLISH_TO_TARGET(
    P_RUN_ID        VARCHAR,
    P_BATCH_RUN_ID  INTEGER DEFAULT 0
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_nl_count       INTEGER;
    v_nli_count      INTEGER;
    v_nlc_count      INTEGER;
    v_deactivated    INTEGER;
    v_hist_inserted  INTEGER;
BEGIN

    SELECT COUNT(*) INTO v_nl_count  FROM DBT_UDL.WRK_NEWSLETTER;
    SELECT COUNT(*) INTO v_nli_count FROM DBT_UDL.WRK_NEWSLETTER_INTERACTION;
    SELECT COUNT(*) INTO v_nlc_count FROM DBT_UDL.WRK_NEWSLETTER_CATEGORY;

    BEGIN TRANSACTION;

    -- ═══════════════════════════════════════════════════════════════
    -- NEWSLETTER: HIST-as-master SCD-2 pattern
    -- Step 1: Deactivate superseded records in NEWSLETTER_HIST
    -- ═══════════════════════════════════════════════════════════════
    UPDATE UDL.NEWSLETTER_HIST h
    SET    h.ACTIVE_FLAG      = FALSE,
           h.INACTIVE_DATE    = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
           h.UPDATED_BY       = CURRENT_USER(),
           h.UPDATED_DATETIME = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
    WHERE  h.ACTIVE_FLAG = TRUE
      AND  EXISTS (
               SELECT 1
               FROM   DBT_UDL.WRK_NEWSLETTER w
               WHERE  w.TENANT_CODE = h.TENANT_CODE
                 AND  w.CODE        = h.CODE
           );

    v_deactivated := SQLROWCOUNT;

    -- Step 2: Insert new active records from WRK into NEWSLETTER_HIST
    INSERT INTO UDL.NEWSLETTER_HIST (
        ID, DATA_SOURCE_CODE, TENANT_CODE, STAGING_ID, CODE, NAME, SUBJECT,
        SENDER_ADDRESS, SEND_AS_EMAIL, SEND_AS_SMS, SEND_AS_MS_TEAMS_MESSAGE,
        SEND_AS_SLACK_MESSAGE, SEND_AS_INTRANET, SCHEDULED_AT, SENT_AT,
        NEWSLETTER_CREATED_BY_CODE, NEWSLETTER_UPDATED_BY_CODE,
        NEWSLETTER_CREATED_DATETIME, NEWSLETTER_UPDATED_DATETIME,
        STATUS_CODE, CATEGORY_CODE, TEMPLATE_CODE, THEME_CODE,
        IS_ARCHIVED, SEND_AS_TIMEZONE_AWARE_SCHEDULE, REPLY_TO_EMAIL_ADDRESS,
        RECIPIENT_INFO, RECIPIENT_TYPE_CODE, IS_DELETED, DELETED_NOTE,
        DELETED_DATETIME, ACTIVE_FLAG, ACTIVE_DATE, INACTIVE_DATE,
        CREATED_BY, CREATED_DATETIME, UPDATED_BY, UPDATED_DATETIME,
        HASH_VALUE, RECIPIENT_NAME, ACTUAL_DELIVERY_SYSTEM_TYPE,
        BATCH_RUN_ID, DBT_LOADED_AT, DBT_RUN_ID, DBT_BATCH_ID, DBT_SOURCE_MODEL,
        DBT_ENVIRONMENT, PUBLISHED_BY_RUN_ID, PUBLISHED_AT
    )
    SELECT
        w.ID, w.DATA_SOURCE_CODE, w.TENANT_CODE, w.STAGING_ID, w.CODE, w.NAME,
        w.SUBJECT, w.SENDER_ADDRESS, w.SEND_AS_EMAIL, w.SEND_AS_SMS,
        w.SEND_AS_MS_TEAMS_MESSAGE, w.SEND_AS_SLACK_MESSAGE, w.SEND_AS_INTRANET,
        w.SCHEDULED_AT, w.SENT_AT, w.NEWSLETTER_CREATED_BY_CODE,
        w.NEWSLETTER_UPDATED_BY_CODE, w.NEWSLETTER_CREATED_DATETIME,
        w.NEWSLETTER_UPDATED_DATETIME, w.STATUS_CODE, w.CATEGORY_CODE,
        w.TEMPLATE_CODE, w.THEME_CODE, w.IS_ARCHIVED,
        w.SEND_AS_TIMEZONE_AWARE_SCHEDULE, w.REPLY_TO_EMAIL_ADDRESS,
        w.RECIPIENT_INFO, w.RECIPIENT_TYPE_CODE, w.IS_DELETED, w.DELETED_NOTE,
        w.DELETED_DATETIME, TRUE, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, NULL,
        CURRENT_USER(), CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, NULL, NULL,
        w.HASH_VALUE, w.RECIPIENT_NAME, w.ACTUAL_DELIVERY_SYSTEM_TYPE,
        w.BATCH_RUN_ID, w.DBT_LOADED_AT, w.DBT_RUN_ID, w.DBT_BATCH_ID, w.DBT_SOURCE_MODEL,
        w.DBT_ENVIRONMENT, :P_RUN_ID, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
    FROM DBT_UDL.WRK_NEWSLETTER w;

    v_hist_inserted := SQLROWCOUNT;

    -- Step 3: Derive UDL.NEWSLETTER from active HIST records (preserves Time Travel)
    TRUNCATE TABLE UDL.NEWSLETTER;
    INSERT INTO UDL.NEWSLETTER
    SELECT
        ID, DATA_SOURCE_CODE, TENANT_CODE, STAGING_ID, CODE, NAME, SUBJECT,
        SENDER_ADDRESS, SEND_AS_EMAIL, SEND_AS_SMS, SEND_AS_MS_TEAMS_MESSAGE,
        SEND_AS_SLACK_MESSAGE, SEND_AS_INTRANET, SCHEDULED_AT, SENT_AT,
        NEWSLETTER_CREATED_BY_CODE, NEWSLETTER_UPDATED_BY_CODE,
        NEWSLETTER_CREATED_DATETIME, NEWSLETTER_UPDATED_DATETIME,
        STATUS_CODE, CATEGORY_CODE, TEMPLATE_CODE, THEME_CODE,
        IS_ARCHIVED, SEND_AS_TIMEZONE_AWARE_SCHEDULE, REPLY_TO_EMAIL_ADDRESS,
        RECIPIENT_INFO, RECIPIENT_TYPE_CODE, IS_DELETED, DELETED_NOTE,
        DELETED_DATETIME, ACTIVE_FLAG, ACTIVE_DATE, INACTIVE_DATE,
        CREATED_BY, CREATED_DATETIME, UPDATED_BY, UPDATED_DATETIME,
        HASH_VALUE, RECIPIENT_NAME, ACTUAL_DELIVERY_SYSTEM_TYPE,
        BATCH_RUN_ID, DBT_LOADED_AT, DBT_RUN_ID, DBT_BATCH_ID, DBT_SOURCE_MODEL,
        DBT_ENVIRONMENT
    FROM UDL.NEWSLETTER_HIST
    WHERE ACTIVE_FLAG = TRUE;

    -- ═══════════════════════════════════════════════════════════════
    -- INTERACTION + CATEGORY: delta MERGE + stamp publish traceability
    -- ═══════════════════════════════════════════════════════════════
    CALL UDL_BATCH_PROCESS.PRC_MERGE_PUBLISH(
        'DBT_UDL', 'WRK_NEWSLETTER_INTERACTION', 'UDL', 'NEWSLETTER_INTERACTION', :P_RUN_ID
    );
    CALL UDL_BATCH_PROCESS.PRC_MERGE_PUBLISH(
        'DBT_UDL', 'WRK_NEWSLETTER_CATEGORY', 'UDL', 'NEWSLETTER_CATEGORY', :P_RUN_ID
    );

    COMMIT;

    -- Mark reprocess requests as completed
    UPDATE UDL_BATCH_PROCESS.REPROCESS_REQUEST
    SET    STATUS              = 'COMPLETED',
           PROCESSED_BY_RUN_ID = :P_RUN_ID,
           PROCESSED_AT        = CURRENT_TIMESTAMP(),
           BATCH_RUN_ID        = :P_BATCH_RUN_ID
    WHERE  STATUS = 'PENDING';

    INSERT INTO UDL_BATCH_PROCESS.REPROCESS_REQUEST_HIST
    SELECT * FROM UDL_BATCH_PROCESS.REPROCESS_REQUEST
    WHERE STATUS = 'COMPLETED';

    DELETE FROM UDL_BATCH_PROCESS.REPROCESS_REQUEST
    WHERE STATUS = 'COMPLETED';

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
        'hist_as_master',
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
        'status',           'success',
        'run_id',           P_RUN_ID,
        'batch_run_id',     P_BATCH_RUN_ID,
        'strategy',         'hist_as_master',
        'nl_delta',         v_nl_count,
        'nl_deactivated',   v_deactivated,
        'nl_hist_inserted', v_hist_inserted,
        'interaction',      v_nli_count,
        'category',         v_nlc_count
    )::VARCHAR;

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RETURN OBJECT_CONSTRUCT(
            'status',       'failed',
            'run_id',       P_RUN_ID,
            'batch_run_id', P_BATCH_RUN_ID,
            'error',        SQLERRM
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
-- 3. Safety net: ensure NEWSLETTER_HIST has all required columns
--    The DDL in account_bootstrap.sql creates these, but ALTER IF NOT EXISTS
--    guards against partial setups.
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
