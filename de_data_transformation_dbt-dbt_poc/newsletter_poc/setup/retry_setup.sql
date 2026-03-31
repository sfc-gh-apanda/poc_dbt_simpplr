-- ═══════════════════════════════════════════════════════════════════════════════
-- SMART RETRY — Setup for failure-aware restartability on Snowflake Native dbt
-- Run ONCE after audit_setup.sql and publish_archive_setup.sql
--
-- Components:
--   1. DBT_MODEL_MANIFEST  — registry of all expected dbt models
--   2. V_RETRY_CANDIDATES  — view showing models that need retry for a given run
--   3. V_LAST_RUN_FAILURES — quick-look view of the most recent failed run
--   4. PRC_DBT_SMART_RETRY — procedure that builds and optionally executes retry
--
-- Why this exists:
--   Snowflake Native dbt (EXECUTE DBT PROJECT) has no persistent target/ dir,
--   so dbt retry / --state / result:fail+ are unavailable. This module uses
--   DBT_MODEL_LOG (populated by log_model_execution + log_failed_models macros)
--   as a substitute for run_results.json.
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE R_DEPLOYMENT_ADMIN_DEV;
USE DATABASE COMMON_TENANT_DEV;
USE SCHEMA DBT_EXECUTION_RUN_STATS;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 1. DBT_MODEL_MANIFEST — static registry of expected models
--    Update this table whenever you add/remove dbt models.
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS DBT_MODEL_MANIFEST (
    model_name          VARCHAR(200)    PRIMARY KEY     COMMENT 'dbt model name (matches this.name)',
    model_type          VARCHAR(20)                     COMMENT 'staging / intermediate / marts / snapshot / sentinel',
    schema_name         VARCHAR(100)                    COMMENT 'Target schema',
    is_critical         BOOLEAN         DEFAULT TRUE    COMMENT 'Must succeed for publish to proceed',
    depends_on          VARCHAR(1000)                   COMMENT 'Comma-separated upstream model names',
    retry_with_suffix   BOOLEAN         DEFAULT TRUE    COMMENT 'Use model+ (with downstream) when retrying',
    created_at          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

MERGE INTO DBT_MODEL_MANIFEST t
USING (
    SELECT column1 AS model_name, column2 AS model_type, column3 AS schema_name,
           column4 AS is_critical, column5 AS depends_on, column6 AS retry_with_suffix
    FROM VALUES
        ('stg_newsletter',                     'staging',       'UDL',     TRUE,  NULL,                                                          TRUE),
        ('stg_newsletter_recipient',           'staging',       'UDL',     TRUE,  NULL,                                                          TRUE),
        ('stg_newsletter_interaction',         'staging',       'UDL',     TRUE,  NULL,                                                          TRUE),
        ('stg_newsletter_interaction_summary', 'staging',       'UDL',     TRUE,  NULL,                                                          TRUE),
        ('stg_newsletter_category',            'staging',       'UDL',     TRUE,  NULL,                                                          TRUE),
        ('int_newsletter_joined',              'intermediate',  'UDL',     TRUE,  'stg_newsletter,stg_newsletter_recipient,stg_newsletter_interaction_summary', TRUE),
        ('wrk_newsletter',                     'marts',         'DBT_UDL', TRUE,  'int_newsletter_joined',                                       TRUE),
        ('wrk_newsletter_interaction',         'marts',         'DBT_UDL', TRUE,  'stg_newsletter_interaction',                                  TRUE),
        ('wrk_newsletter_category',            'marts',         'DBT_UDL', TRUE,  'stg_newsletter_category',                                     TRUE),
        ('snap_newsletter',                    'snapshot',      'DBT_UDL', FALSE, 'wrk_newsletter',                                              TRUE),
        ('pipeline_complete',                  'sentinel',      'DBT_UDL', TRUE,  'wrk_newsletter,wrk_newsletter_interaction,wrk_newsletter_category', FALSE)
) s ON t.model_name = s.model_name
WHEN MATCHED THEN UPDATE SET
    model_type = s.model_type, schema_name = s.schema_name,
    is_critical = s.is_critical, depends_on = s.depends_on,
    retry_with_suffix = s.retry_with_suffix
WHEN NOT MATCHED THEN INSERT (model_name, model_type, schema_name, is_critical, depends_on, retry_with_suffix)
    VALUES (s.model_name, s.model_type, s.schema_name, s.is_critical, s.depends_on, s.retry_with_suffix);


-- ═══════════════════════════════════════════════════════════════════════════════
-- 2. V_RETRY_CANDIDATES — models that failed / were skipped in a given run
--    Shows expected models (from manifest) that don't have a SUCCESS log entry.
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW V_RETRY_CANDIDATES AS
SELECT
    m.model_name,
    m.model_type,
    m.schema_name,
    m.is_critical,
    m.retry_with_suffix,
    r.run_id,
    r.run_started_at,
    COALESCE(ml.status, 'NOT_RUN')     AS actual_status,
    ml.error_message
FROM DBT_MODEL_MANIFEST m
CROSS JOIN (
    SELECT run_id, run_started_at
    FROM DBT_RUN_LOG
    WHERE run_status IN ('FAILED', 'ERROR', 'RUNNING')
    ORDER BY run_started_at DESC
    LIMIT 1
) r
LEFT JOIN DBT_MODEL_LOG ml
    ON ml.run_id = r.run_id
   AND ml.model_name = m.model_name
   AND ml.status = 'SUCCESS'
WHERE ml.log_id IS NULL
ORDER BY
    CASE m.model_type
        WHEN 'staging'       THEN 1
        WHEN 'intermediate'  THEN 2
        WHEN 'marts'         THEN 3
        WHEN 'snapshot'      THEN 4
        WHEN 'sentinel'      THEN 5
        ELSE 9
    END;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 3. V_LAST_RUN_FAILURES — quick look at the most recent failed run
--    Shows all models logged with FAIL/ERROR/SKIPPED status.
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW V_LAST_RUN_FAILURES AS
SELECT
    r.run_id,
    r.run_started_at,
    r.run_status,
    r.run_duration_seconds,
    ml.model_name,
    ml.status,
    ml.error_message,
    ml.materialization,
    ml.started_at  AS model_started_at
FROM DBT_RUN_LOG r
JOIN DBT_MODEL_LOG ml
    ON ml.run_id = r.run_id
   AND ml.status IN ('FAIL', 'ERROR', 'SKIPPED')
WHERE r.run_started_at = (
    SELECT MAX(run_started_at)
    FROM DBT_RUN_LOG
    WHERE run_status IN ('FAILED', 'ERROR')
)
ORDER BY ml.started_at;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 4. PRC_DBT_SMART_RETRY
--    Reads DBT_MODEL_MANIFEST + DBT_MODEL_LOG to identify failed/skipped models,
--    builds a dbt --select clause, and optionally executes EXECUTE DBT PROJECT.
--
--    Parameters:
--      P_STAGE        — Snowflake stage (e.g., '@my_db.my_schema.my_stage')
--      P_PROJECT_ROOT — dbt project root in stage (e.g., '/newsletter_poc')
--      P_TARGET       — dbt target profile (e.g., 'dev')
--      P_RUN_ID       — (optional) specific run_id to retry; NULL = latest failed
--      P_DRY_RUN      — TRUE = return the command without executing
--
--    Returns:  JSON with status, the built command, and the list of models.
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE UDL_BATCH_PROCESS.PRC_DBT_SMART_RETRY(
    P_STAGE        VARCHAR,
    P_PROJECT_ROOT VARCHAR,
    P_TARGET       VARCHAR,
    P_RUN_ID       VARCHAR DEFAULT NULL,
    P_DRY_RUN      BOOLEAN DEFAULT FALSE
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id          VARCHAR;
    v_run_started     TIMESTAMP_NTZ;
    v_select_models   VARCHAR DEFAULT '';
    v_model_count     INTEGER DEFAULT 0;
    v_failed_list     VARCHAR DEFAULT '';
    v_skipped_list    VARCHAR DEFAULT '';
    v_notrun_list     VARCHAR DEFAULT '';
    v_dbt_command     VARCHAR;
    v_full_command    VARCHAR;
    c_candidates CURSOR FOR
        SELECT
            m.model_name,
            m.retry_with_suffix,
            COALESCE(ml_fail.status, 'NOT_RUN') AS actual_status
        FROM DBT_EXECUTION_RUN_STATS.DBT_MODEL_MANIFEST m
        LEFT JOIN DBT_EXECUTION_RUN_STATS.DBT_MODEL_LOG ml_success
            ON ml_success.run_id = v_run_id
           AND ml_success.model_name = m.model_name
           AND ml_success.status = 'SUCCESS'
        LEFT JOIN DBT_EXECUTION_RUN_STATS.DBT_MODEL_LOG ml_fail
            ON ml_fail.run_id = v_run_id
           AND ml_fail.model_name = m.model_name
           AND ml_fail.status IN ('FAIL', 'ERROR', 'SKIPPED')
        WHERE ml_success.log_id IS NULL
          AND m.model_type != 'intermediate'
        ORDER BY
            CASE m.model_type
                WHEN 'staging'  THEN 1
                WHEN 'marts'    THEN 2
                WHEN 'snapshot' THEN 3
                WHEN 'sentinel' THEN 4
                ELSE 9
            END;
BEGIN

    -- ── Step 1: Resolve the target run_id ──────────────────────────
    IF (P_RUN_ID IS NOT NULL) THEN
        v_run_id := P_RUN_ID;
        SELECT run_started_at INTO v_run_started
        FROM DBT_EXECUTION_RUN_STATS.DBT_RUN_LOG
        WHERE run_id = v_run_id;
    ELSE
        SELECT run_id, run_started_at
        INTO v_run_id, v_run_started
        FROM DBT_EXECUTION_RUN_STATS.DBT_RUN_LOG
        WHERE run_status IN ('FAILED', 'ERROR', 'RUNNING')
        ORDER BY run_started_at DESC
        LIMIT 1;
    END IF;

    IF (v_run_id IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'status',  'no_retry_needed',
            'message', 'No failed or stale runs found in DBT_RUN_LOG.'
        )::VARCHAR;
    END IF;

    -- ── Step 2: Build the --select clause from candidates ──────────
    OPEN c_candidates;
    FOR rec IN c_candidates DO
        LET model_selector VARCHAR := rec.model_name;
        IF (rec.retry_with_suffix) THEN
            model_selector := rec.model_name || '+';
        END IF;

        IF (v_select_models != '') THEN
            v_select_models := v_select_models || ' ';
        END IF;
        v_select_models := v_select_models || model_selector;
        v_model_count   := v_model_count + 1;

        CASE rec.actual_status
            WHEN 'FAIL'    THEN v_failed_list  := v_failed_list  || rec.model_name || ', ';
            WHEN 'ERROR'   THEN v_failed_list  := v_failed_list  || rec.model_name || ', ';
            WHEN 'SKIPPED' THEN v_skipped_list := v_skipped_list || rec.model_name || ', ';
            ELSE                v_notrun_list  := v_notrun_list  || rec.model_name || ', ';
        END CASE;
    END FOR;
    CLOSE c_candidates;

    IF (v_model_count = 0) THEN
        RETURN OBJECT_CONSTRUCT(
            'status',      'no_retry_needed',
            'run_id',      v_run_id,
            'run_started', v_run_started,
            'message',     'All manifest models have SUCCESS status for this run.'
        )::VARCHAR;
    END IF;

    -- ── Step 3: Build the full EXECUTE DBT PROJECT command ─────────
    v_dbt_command  := 'build --select ' || v_select_models || ' --target ' || P_TARGET;
    v_full_command := 'EXECUTE DBT PROJECT FROM ' || P_STAGE
                   || ' PROJECT_ROOT = ''' || P_PROJECT_ROOT || ''''
                   || ' ARGS = ''' || v_dbt_command || '''';

    -- ── Step 4: Execute or return as dry-run ──────────────────────
    IF (NOT P_DRY_RUN) THEN
        BEGIN
            EXECUTE IMMEDIATE v_full_command;
        EXCEPTION
            WHEN OTHER THEN
                RETURN OBJECT_CONSTRUCT(
                    'status',         'retry_execution_failed',
                    'retry_run_id',   v_run_id,
                    'command',        v_full_command,
                    'models_retried', v_model_count,
                    'failed',         TRIM(v_failed_list, ', '),
                    'skipped',        TRIM(v_skipped_list, ', '),
                    'not_run',        TRIM(v_notrun_list, ', '),
                    'error',          SQLERRM
                )::VARCHAR;
        END;
    END IF;

    RETURN OBJECT_CONSTRUCT(
        'status',         IFF(P_DRY_RUN, 'dry_run', 'retry_executed'),
        'original_run_id', v_run_id,
        'original_run_at', v_run_started,
        'command',         v_full_command,
        'dbt_args',        v_dbt_command,
        'models_retried',  v_model_count,
        'failed',          TRIM(v_failed_list, ', '),
        'skipped',         TRIM(v_skipped_list, ', '),
        'not_run',         TRIM(v_notrun_list, ', ')
    )::VARCHAR;

END;
$$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 5. PRC_DBT_FULL_RUN_WITH_RETRY
--    Convenience wrapper: runs a full dbt build, checks for failures,
--    and automatically retries failed models up to N times.
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE UDL_BATCH_PROCESS.PRC_DBT_FULL_RUN_WITH_RETRY(
    P_STAGE        VARCHAR,
    P_PROJECT_ROOT VARCHAR,
    P_TARGET       VARCHAR,
    P_MAX_RETRIES  INTEGER DEFAULT 2,
    P_DBT_VARS     VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_attempt      INTEGER DEFAULT 0;
    v_run_status   VARCHAR;
    v_retry_result VARCHAR;
    v_dbt_args     VARCHAR;
    v_full_command VARCHAR;
    v_results      ARRAY DEFAULT ARRAY_CONSTRUCT();
BEGIN

    -- ── Initial full run ──────────────────────────────────────────
    v_dbt_args := 'build --target ' || P_TARGET;
    IF (P_DBT_VARS IS NOT NULL AND P_DBT_VARS != '') THEN
        v_dbt_args := v_dbt_args || ' --vars ''' || P_DBT_VARS || '''';
    END IF;

    v_full_command := 'EXECUTE DBT PROJECT FROM ' || P_STAGE
                   || ' PROJECT_ROOT = ''' || P_PROJECT_ROOT || ''''
                   || ' ARGS = ''' || v_dbt_args || '''';

    BEGIN
        EXECUTE IMMEDIATE v_full_command;
        v_results := ARRAY_APPEND(v_results,
            OBJECT_CONSTRUCT('attempt', 0, 'type', 'full_run', 'status', 'completed'));
    EXCEPTION
        WHEN OTHER THEN
            v_results := ARRAY_APPEND(v_results,
                OBJECT_CONSTRUCT('attempt', 0, 'type', 'full_run', 'status', 'failed', 'error', SQLERRM));
    END;

    -- ── Check if retry is needed ──────────────────────────────────
    SELECT run_status INTO v_run_status
    FROM DBT_EXECUTION_RUN_STATS.DBT_RUN_LOG
    ORDER BY run_started_at DESC
    LIMIT 1;

    -- ── Retry loop ────────────────────────────────────────────────
    WHILE (v_run_status IN ('FAILED', 'ERROR') AND v_attempt < P_MAX_RETRIES) DO
        v_attempt := v_attempt + 1;

        CALL UDL_BATCH_PROCESS.PRC_DBT_SMART_RETRY(
            P_STAGE, P_PROJECT_ROOT, P_TARGET, NULL, FALSE
        ) INTO v_retry_result;

        v_results := ARRAY_APPEND(v_results,
            OBJECT_CONSTRUCT('attempt', v_attempt, 'type', 'smart_retry', 'result', PARSE_JSON(v_retry_result)));

        IF (PARSE_JSON(v_retry_result):status::VARCHAR = 'no_retry_needed') THEN
            v_run_status := 'SUCCESS';
        ELSE
            SELECT run_status INTO v_run_status
            FROM DBT_EXECUTION_RUN_STATS.DBT_RUN_LOG
            ORDER BY run_started_at DESC
            LIMIT 1;
        END IF;
    END WHILE;

    RETURN OBJECT_CONSTRUCT(
        'final_status',   v_run_status,
        'total_attempts', v_attempt + 1,
        'max_retries',    P_MAX_RETRIES,
        'attempts',       v_results
    )::VARCHAR;

END;
$$;
