-- ═══════════════════════════════════════════════════════════════════════════════
-- dbt ARTIFACT-BASED TEST & BUILD LOGGING
-- Run ONCE after audit_setup.sql
--
-- Leverages SYSTEM$LOCATE_DBT_ARTIFACTS() to retrieve manifest.json and
-- run_results.json after EXECUTE DBT PROJECT, then parses them for:
--   - Granular test results (test type, column, object, pass/fail, failures)
--   - Model execution results (status, execution time, rows affected)
--
-- Components:
--   1. File format + internal stage for artifact storage
--   2. DBT_TEST_RESULTS table — granular test outcome logging
--   3. DBT_BUILD_RESULTS table — model/snapshot/seed execution outcomes
--   4. PRC_DBT_EXECUTE_AND_LOG_TESTS — run dbt test + log results
--   5. PRC_DBT_EXECUTE_AND_LOG_BUILD — run dbt build + log model & test results
--   6. PRC_DBT_LOG_ARTIFACTS — utility to parse artifacts from any prior run
--   7. Monitoring views for test analytics
--
-- Based on Snowflake SKE: "Automating dbt Test Execution and Result Logging"
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;
USE DATABASE COMMON_TENANT_DEV;
USE SCHEMA DBT_EXECUTION_RUN_STATS;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 1. INFRASTRUCTURE — File format, stage, tables
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE FILE FORMAT IF NOT EXISTS DBT_EXECUTION_RUN_STATS.DBT_JSON_FORMAT
    TYPE = 'JSON';

CREATE STAGE IF NOT EXISTS DBT_EXECUTION_RUN_STATS.DBT_ARTIFACTS
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    FILE_FORMAT = DBT_EXECUTION_RUN_STATS.DBT_JSON_FORMAT
    COMMENT = 'Stores dbt manifest.json and run_results.json per execution';

CREATE TABLE IF NOT EXISTS DBT_EXECUTION_RUN_STATS.DBT_TEST_RESULTS (
    query_id            VARCHAR(50)                     COMMENT 'Snowflake query ID for the EXECUTE DBT PROJECT call',
    run_id              VARCHAR(50)                     COMMENT 'Mapped to dbt invocation_id when available',
    unique_id           VARCHAR(500)                    COMMENT 'dbt test unique_id (e.g. test.newsletter_poc.not_null_wrk_newsletter_id)',
    test_name           VARCHAR(200)                    COMMENT 'Extracted test name',
    test_type           VARCHAR(100)                    COMMENT 'Test type: not_null, unique, accepted_values, relationships, custom',
    column_name         VARCHAR(200)                    COMMENT 'Column being tested (if applicable)',
    object_name         VARCHAR(200)                    COMMENT 'Table/model/source being tested',
    object_database     VARCHAR(100)                    COMMENT 'Database of tested object',
    object_schema       VARCHAR(100)                    COMMENT 'Schema of tested object',
    status              VARCHAR(20)                     COMMENT 'pass / fail / warn / error',
    failures            INTEGER                         COMMENT 'Number of failing rows',
    execution_time      FLOAT                           COMMENT 'Test execution duration in seconds',
    message             VARCHAR(4000)                   COMMENT 'Error/failure message details',
    run_timestamp       TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS DBT_EXECUTION_RUN_STATS.DBT_BUILD_RESULTS (
    query_id            VARCHAR(50)                     COMMENT 'Snowflake query ID for the EXECUTE DBT PROJECT call',
    run_id              VARCHAR(50)                     COMMENT 'Mapped to dbt invocation_id when available',
    unique_id           VARCHAR(500)                    COMMENT 'dbt node unique_id (e.g. model.newsletter_poc.wrk_newsletter)',
    node_name           VARCHAR(200)                    COMMENT 'Model/snapshot/seed name',
    node_type           VARCHAR(50)                     COMMENT 'model / snapshot / seed / test',
    status              VARCHAR(20)                     COMMENT 'success / error / fail / skipped',
    execution_time      FLOAT                           COMMENT 'Execution duration in seconds',
    rows_affected       INTEGER                         COMMENT 'Rows affected (from adapter_response)',
    message             VARCHAR(4000)                   COMMENT 'Error/status message',
    run_timestamp       TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);


-- ═══════════════════════════════════════════════════════════════════════════════
-- 2. PRC_DBT_LOG_ARTIFACTS — Parse artifacts from a prior run
--    Utility: given a query_id, locate artifacts, copy to stage, parse results.
--    Can be called standalone or from the execute procedures.
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE UDL_BATCH_PROCESS.PRC_DBT_LOG_ARTIFACTS(
    P_QUERY_ID  VARCHAR,
    P_RUN_ID    VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_artifacts_path  VARCHAR;
    v_run_date        VARCHAR;
    v_stage_path      VARCHAR;
    v_tests_inserted  INTEGER DEFAULT 0;
    v_builds_inserted INTEGER DEFAULT 0;
    v_copy_sql        VARCHAR;
    v_insert_sql      VARCHAR;
BEGIN
    v_run_date   := TO_CHAR(CURRENT_DATE(), 'YYYYMMDD');
    v_stage_path := '@COMMON_TENANT_DEV.DBT_EXECUTION_RUN_STATS.DBT_ARTIFACTS/'
                 || v_run_date || '/' || P_QUERY_ID;

    -- ── Locate artifacts ──────────────────────────────────────────
    BEGIN
        SELECT SYSTEM$LOCATE_DBT_ARTIFACTS(:P_QUERY_ID) INTO v_artifacts_path;
    EXCEPTION
        WHEN OTHER THEN
            RETURN OBJECT_CONSTRUCT(
                'status', 'error',
                'step',   'locate_artifacts',
                'query_id', P_QUERY_ID,
                'error',  SQLERRM
            )::VARCHAR;
    END;

    -- ── Copy to stage ─────────────────────────────────────────────
    BEGIN
        v_copy_sql := 'COPY FILES INTO ' || v_stage_path || '/ FROM ''' || v_artifacts_path || '''';
        EXECUTE IMMEDIATE v_copy_sql;
    EXCEPTION
        WHEN OTHER THEN
            RETURN OBJECT_CONSTRUCT(
                'status', 'error',
                'step',   'copy_to_stage',
                'query_id', P_QUERY_ID,
                'error',  SQLERRM
            )::VARCHAR;
    END;

    -- ── Parse and insert TEST results ─────────────────────────────
    BEGIN
        v_insert_sql := '
        INSERT INTO COMMON_TENANT_DEV.DBT_EXECUTION_RUN_STATS.DBT_TEST_RESULTS (
            query_id, run_id, unique_id, test_name, test_type, column_name,
            object_name, object_database, object_schema,
            status, failures, execution_time, message
        )
        WITH test_nodes AS (
            SELECT
                n.key                                           AS test_key,
                n.value:test_metadata:name::STRING              AS test_type,
                n.value:test_metadata:kwargs:column_name::STRING AS column_name,
                d.value::STRING                                 AS depends_on_node
            FROM ' || v_stage_path || '/target/manifest.json m,
                 LATERAL FLATTEN(input => m.$1:nodes) n,
                 LATERAL FLATTEN(input => n.value:depends_on:nodes) d
            WHERE n.key LIKE ''test.%''
        ),
        model_nodes AS (
            SELECT
                n.key                                           AS node_key,
                REPLACE(n.value:relation_name::STRING, ''"'', '''') AS relation_name
            FROM ' || v_stage_path || '/target/manifest.json m,
                 LATERAL FLATTEN(input => m.$1:nodes) n
            WHERE n.key LIKE ''model.%''
               OR n.key LIKE ''source.%''
               OR n.key LIKE ''seed.%''
               OR n.key LIKE ''snapshot.%''
        ),
        test_to_object AS (
            SELECT
                tn.test_key, tn.test_type, tn.column_name,
                mn.relation_name, mn.node_key
            FROM test_nodes tn
            LEFT JOIN model_nodes mn ON tn.depends_on_node = mn.node_key
        ),
        test_results AS (
            SELECT
                r.value:unique_id::STRING       AS unique_id,
                r.value:status::STRING          AS status,
                r.value:failures::INT           AS failures,
                r.value:execution_time::FLOAT   AS execution_time,
                r.value:message::STRING         AS message
            FROM ' || v_stage_path || '/target/run_results.json rr,
                 LATERAL FLATTEN(input => rr.$1:results) r
            WHERE r.value:unique_id::STRING LIKE ''test.%''
        )
        SELECT
            ''' || P_QUERY_ID || ''',
            ' || IFF(P_RUN_ID IS NOT NULL, '''' || P_RUN_ID || '''', 'NULL') || ',
            tr.unique_id,
            SPLIT_PART(tr.unique_id, ''.'', 3)                      AS test_name,
            tto.test_type,
            tto.column_name,
            COALESCE(
                NULLIF(SPLIT_PART(tto.relation_name, ''.'', 3), ''''),
                SPLIT_PART(tto.node_key, ''.'', 3)
            )                                                       AS object_name,
            NULLIF(SPLIT_PART(tto.relation_name, ''.'', 1), '''')   AS object_database,
            NULLIF(SPLIT_PART(tto.relation_name, ''.'', 2), '''')   AS object_schema,
            tr.status, tr.failures, tr.execution_time, tr.message
        FROM test_results tr
        LEFT JOIN test_to_object tto ON tr.unique_id = tto.test_key';

        EXECUTE IMMEDIATE v_insert_sql;
        v_tests_inserted := SQLROWCOUNT;
    EXCEPTION
        WHEN OTHER THEN
            -- Tests may not exist in a build-only run; continue
            v_tests_inserted := 0;
    END;

    -- ── Parse and insert BUILD (model/snapshot/seed) results ──────
    BEGIN
        v_insert_sql := '
        INSERT INTO COMMON_TENANT_DEV.DBT_EXECUTION_RUN_STATS.DBT_BUILD_RESULTS (
            query_id, run_id, unique_id, node_name, node_type,
            status, execution_time, rows_affected, message
        )
        SELECT
            ''' || P_QUERY_ID || ''',
            ' || IFF(P_RUN_ID IS NOT NULL, '''' || P_RUN_ID || '''', 'NULL') || ',
            r.value:unique_id::STRING,
            SPLIT_PART(r.value:unique_id::STRING, ''.'', 3)    AS node_name,
            SPLIT_PART(r.value:unique_id::STRING, ''.'', 1)    AS node_type,
            r.value:status::STRING,
            r.value:execution_time::FLOAT,
            r.value:adapter_response:rows_affected::INT,
            r.value:message::STRING
        FROM ' || v_stage_path || '/target/run_results.json rr,
             LATERAL FLATTEN(input => rr.$1:results) r
        WHERE r.value:unique_id::STRING NOT LIKE ''test.%''';

        EXECUTE IMMEDIATE v_insert_sql;
        v_builds_inserted := SQLROWCOUNT;
    EXCEPTION
        WHEN OTHER THEN
            v_builds_inserted := 0;
    END;

    RETURN OBJECT_CONSTRUCT(
        'status',          'success',
        'query_id',        P_QUERY_ID,
        'run_id',          P_RUN_ID,
        'stage_path',      v_stage_path,
        'tests_logged',    v_tests_inserted,
        'builds_logged',   v_builds_inserted
    )::VARCHAR;
END;
$$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 3. PRC_DBT_EXECUTE_AND_LOG_TESTS — Run dbt test + capture results
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE UDL_BATCH_PROCESS.PRC_DBT_EXECUTE_AND_LOG_TESTS(
    P_DBT_PROJECT  VARCHAR,
    P_DBT_ARGS     VARCHAR DEFAULT ''
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_query_id      VARCHAR;
    v_dbt_success   BOOLEAN DEFAULT TRUE;
    v_dbt_error     VARCHAR DEFAULT '';
    v_log_result    VARCHAR;
    v_exec_sql      VARCHAR;
BEGIN
    -- ── Execute dbt test ──────────────────────────────────────────
    BEGIN
        v_exec_sql := 'EXECUTE DBT PROJECT ' || P_DBT_PROJECT
                    || ' ARGS = ''test ' || P_DBT_ARGS || '''';
        EXECUTE IMMEDIATE v_exec_sql;
        v_query_id := LAST_QUERY_ID();
    EXCEPTION
        WHEN OTHER THEN
            v_dbt_success := FALSE;
            v_dbt_error   := SQLERRM;
            v_query_id    := LAST_QUERY_ID();
    END;

    IF (v_query_id IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'error',
            'step',   'execute_dbt_test',
            'error',  v_dbt_error
        )::VARCHAR;
    END IF;

    -- ── Log artifacts ─────────────────────────────────────────────
    CALL UDL_BATCH_PROCESS.PRC_DBT_LOG_ARTIFACTS(v_query_id, NULL) INTO v_log_result;

    RETURN OBJECT_CONSTRUCT(
        'status',       IFF(v_dbt_success, 'success', 'tests_had_failures'),
        'query_id',     v_query_id,
        'dbt_success',  v_dbt_success,
        'dbt_error',    v_dbt_error,
        'log_result',   PARSE_JSON(v_log_result)
    )::VARCHAR;
END;
$$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 4. PRC_DBT_EXECUTE_AND_LOG_BUILD — Run dbt build + capture all results
--    This is the primary execution procedure. Runs build (models + tests +
--    snapshots), then captures both model outcomes and test outcomes from
--    the artifacts. Feeds accurate data to PRC_DBT_SMART_RETRY.
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE UDL_BATCH_PROCESS.PRC_DBT_EXECUTE_AND_LOG_BUILD(
    P_DBT_PROJECT   VARCHAR,
    P_DBT_ARGS      VARCHAR DEFAULT '',
    P_DBT_VARS      VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_query_id      VARCHAR;
    v_dbt_success   BOOLEAN DEFAULT TRUE;
    v_dbt_error     VARCHAR DEFAULT '';
    v_log_result    VARCHAR;
    v_exec_sql      VARCHAR;
    v_args          VARCHAR;
BEGIN
    -- ── Build the args string ─────────────────────────────────────
    v_args := 'build ' || P_DBT_ARGS;
    IF (P_DBT_VARS IS NOT NULL AND P_DBT_VARS != '') THEN
        v_args := v_args || ' --vars ''{' || P_DBT_VARS || '}''';
    END IF;

    -- ── Execute dbt build ─────────────────────────────────────────
    BEGIN
        v_exec_sql := 'EXECUTE DBT PROJECT ' || P_DBT_PROJECT
                    || ' ARGS = ''' || v_args || '''';
        EXECUTE IMMEDIATE v_exec_sql;
        v_query_id := LAST_QUERY_ID();
    EXCEPTION
        WHEN OTHER THEN
            v_dbt_success := FALSE;
            v_dbt_error   := SQLERRM;
            v_query_id    := LAST_QUERY_ID();
    END;

    IF (v_query_id IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'error',
            'step',   'execute_dbt_build',
            'error',  v_dbt_error
        )::VARCHAR;
    END IF;

    -- ── Log artifacts (models + tests) ────────────────────────────
    CALL UDL_BATCH_PROCESS.PRC_DBT_LOG_ARTIFACTS(v_query_id, NULL) INTO v_log_result;

    RETURN OBJECT_CONSTRUCT(
        'status',       IFF(v_dbt_success, 'success', 'build_had_failures'),
        'query_id',     v_query_id,
        'dbt_success',  v_dbt_success,
        'dbt_error',    v_dbt_error,
        'log_result',   PARSE_JSON(v_log_result)
    )::VARCHAR;
END;
$$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 5. PRC_DBT_SMART_RETRY_V2 — Enhanced retry using artifact-based results
--    Uses DBT_BUILD_RESULTS (parsed from run_results.json) for ground-truth
--    failure detection, falling back to DBT_MODEL_LOG if artifacts unavailable.
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE UDL_BATCH_PROCESS.PRC_DBT_SMART_RETRY_V2(
    P_DBT_PROJECT   VARCHAR,
    P_DBT_ARGS_BASE VARCHAR DEFAULT '--target dev',
    P_QUERY_ID      VARCHAR DEFAULT NULL,
    P_DRY_RUN       BOOLEAN DEFAULT FALSE
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_query_id        VARCHAR;
    v_select_models   VARCHAR DEFAULT '';
    v_model_count     INTEGER DEFAULT 0;
    v_failed_list     VARCHAR DEFAULT '';
    v_exec_sql        VARCHAR;
    v_log_result      VARCHAR;
    v_new_query_id    VARCHAR;
    c_failed CURSOR FOR
        SELECT node_name, status
        FROM DBT_EXECUTION_RUN_STATS.DBT_BUILD_RESULTS
        WHERE query_id = v_query_id
          AND status IN ('error', 'fail', 'skipped')
          AND node_type IN ('model', 'snapshot', 'seed')
        ORDER BY node_name;
BEGIN

    -- ── Step 1: Resolve query_id ──────────────────────────────────
    IF (P_QUERY_ID IS NOT NULL) THEN
        v_query_id := P_QUERY_ID;
    ELSE
        SELECT query_id INTO v_query_id
        FROM DBT_EXECUTION_RUN_STATS.DBT_BUILD_RESULTS
        WHERE status IN ('error', 'fail')
          AND node_type IN ('model', 'snapshot', 'seed')
        ORDER BY run_timestamp DESC
        LIMIT 1;
    END IF;

    IF (v_query_id IS NULL) THEN
        RETURN OBJECT_CONSTRUCT(
            'status',  'no_retry_needed',
            'message', 'No failed builds found in DBT_BUILD_RESULTS.'
        )::VARCHAR;
    END IF;

    -- ── Step 2: Build --select from failed models ─────────────────
    OPEN c_failed;
    FOR rec IN c_failed DO
        IF (v_select_models != '') THEN
            v_select_models := v_select_models || ' ';
        END IF;
        v_select_models := v_select_models || rec.node_name || '+';
        v_failed_list   := v_failed_list || rec.node_name || ' (' || rec.status || '), ';
        v_model_count   := v_model_count + 1;
    END FOR;
    CLOSE c_failed;

    IF (v_model_count = 0) THEN
        RETURN OBJECT_CONSTRUCT(
            'status',   'no_retry_needed',
            'query_id', v_query_id,
            'message',  'No failed model/snapshot/seed nodes for this query_id.'
        )::VARCHAR;
    END IF;

    -- ── Step 3: Build and optionally execute ──────────────────────
    v_exec_sql := 'EXECUTE DBT PROJECT ' || P_DBT_PROJECT
               || ' ARGS = ''build --select ' || v_select_models || ' ' || P_DBT_ARGS_BASE || '''';

    IF (NOT P_DRY_RUN) THEN
        BEGIN
            EXECUTE IMMEDIATE v_exec_sql;
            v_new_query_id := LAST_QUERY_ID();

            -- Log the retry run's artifacts too
            CALL UDL_BATCH_PROCESS.PRC_DBT_LOG_ARTIFACTS(v_new_query_id, NULL) INTO v_log_result;
        EXCEPTION
            WHEN OTHER THEN
                v_new_query_id := LAST_QUERY_ID();
                RETURN OBJECT_CONSTRUCT(
                    'status',            'retry_failed',
                    'original_query_id', v_query_id,
                    'retry_query_id',    v_new_query_id,
                    'command',           v_exec_sql,
                    'models_retried',    v_model_count,
                    'failed_models',     TRIM(v_failed_list, ', '),
                    'error',             SQLERRM
                )::VARCHAR;
        END;
    END IF;

    RETURN OBJECT_CONSTRUCT(
        'status',            IFF(P_DRY_RUN, 'dry_run', 'retry_executed'),
        'original_query_id', v_query_id,
        'retry_query_id',    v_new_query_id,
        'command',           v_exec_sql,
        'models_retried',    v_model_count,
        'failed_models',     TRIM(v_failed_list, ', ')
    )::VARCHAR;
END;
$$;


-- ═══════════════════════════════════════════════════════════════════════════════
-- 6. MONITORING VIEWS — Test analytics
-- ═══════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW V_TEST_PASS_RATE AS
SELECT
    DATE(run_timestamp)                                             AS test_date,
    COUNT(*)                                                        AS total_tests,
    SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END)               AS passed,
    SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END)               AS failed,
    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END)              AS errors,
    SUM(CASE WHEN status = 'warn' THEN 1 ELSE 0 END)               AS warnings,
    ROUND(SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) * 100.0
          / NULLIF(COUNT(*), 0), 2)                                 AS pass_rate_pct
FROM DBT_TEST_RESULTS
WHERE run_timestamp >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1
ORDER BY 1 DESC;

CREATE OR REPLACE VIEW V_TEST_FAILURES_DETAIL AS
SELECT
    run_timestamp,
    object_database || '.' || object_schema || '.' || object_name   AS full_object_name,
    test_type,
    test_name,
    column_name,
    failures,
    message,
    query_id
FROM DBT_TEST_RESULTS
WHERE status = 'fail'
ORDER BY run_timestamp DESC;

CREATE OR REPLACE VIEW V_TEST_COVERAGE_BY_TYPE AS
SELECT
    test_type,
    COUNT(*)                                                        AS total_executions,
    SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END)               AS passed,
    SUM(CASE WHEN status = 'fail' THEN 1 ELSE 0 END)               AS failed,
    ROUND(AVG(execution_time), 3)                                   AS avg_execution_sec,
    MAX(run_timestamp)                                              AS last_run
FROM DBT_TEST_RESULTS
GROUP BY test_type
ORDER BY failed DESC;

CREATE OR REPLACE VIEW V_BUILD_RESULTS_LATEST AS
SELECT
    br.query_id,
    br.node_name,
    br.node_type,
    br.status,
    br.execution_time,
    br.rows_affected,
    br.message,
    br.run_timestamp
FROM DBT_BUILD_RESULTS br
WHERE br.query_id = (
    SELECT query_id FROM DBT_BUILD_RESULTS
    ORDER BY run_timestamp DESC LIMIT 1
)
ORDER BY
    CASE br.node_type
        WHEN 'seed'     THEN 1
        WHEN 'model'    THEN 2
        WHEN 'snapshot' THEN 3
        ELSE 9
    END,
    br.node_name;

CREATE OR REPLACE VIEW V_BUILD_FAILURES_HISTORY AS
SELECT
    query_id,
    node_name,
    node_type,
    status,
    execution_time,
    message,
    run_timestamp
FROM DBT_BUILD_RESULTS
WHERE status IN ('error', 'fail')
  AND run_timestamp >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY run_timestamp DESC;
