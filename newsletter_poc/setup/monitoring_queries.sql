-- =============================================================
-- SIMPPLR DBT PoC — Monitoring & Dashboard Queries
-- =============================================================
-- Run audit_setup.sql first to create the base tables.
-- These queries can be used as Snowflake dashboard tiles,
-- Snowsight worksheets, or piped into any BI tool.
-- =============================================================

USE DATABASE SIMPPLR_DBT_DEV;
USE SCHEMA SIMPPLR_DBT_AUDIT;


-- ═══════════════════════════════════════════════════════════════
-- 1. RUN METRICS
-- ═══════════════════════════════════════════════════════════════

-- 1a. Run success rate over time (last 30 days)
CREATE OR REPLACE VIEW V_RUN_SUCCESS_RATE AS
SELECT
    DATE(run_started_at)                                     AS run_date,
    COUNT(*)                                                 AS total_runs,
    SUM(IFF(run_status = 'SUCCESS', 1, 0))                   AS success_count,
    SUM(IFF(run_status = 'FAILED',  1, 0))                   AS failed_count,
    ROUND(success_count * 100.0 / NULLIF(total_runs, 0), 1)  AS success_rate_pct,
    ROUND(AVG(run_duration_seconds), 2)                      AS avg_duration_sec,
    ROUND(MAX(run_duration_seconds), 2)                      AS max_duration_sec,
    ROUND(MIN(run_duration_seconds), 2)                      AS min_duration_sec
FROM DBT_RUN_LOG
WHERE run_started_at >= DATEADD('day', -30, CURRENT_DATE())
  AND run_status != 'RUNNING'
GROUP BY 1
ORDER BY 1 DESC;

-- 1b. Run duration trend (detects performance regression)
CREATE OR REPLACE VIEW V_RUN_DURATION_TREND AS
SELECT
    DATE(run_started_at)                                     AS run_date,
    run_duration_seconds,
    ROUND(AVG(run_duration_seconds) OVER (
        ORDER BY run_started_at
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2)                                                    AS rolling_7d_avg_sec,
    models_run,
    models_success,
    models_failed,
    run_id
FROM DBT_RUN_LOG
WHERE run_started_at >= DATEADD('day', -30, CURRENT_DATE())
  AND run_status != 'RUNNING'
ORDER BY 1 DESC;

-- 1c. Latest run status (quick health check)
CREATE OR REPLACE VIEW V_LATEST_RUN_STATUS AS
SELECT
    run_id,
    run_status,
    run_started_at,
    run_ended_at,
    run_duration_seconds,
    models_run,
    models_success,
    models_failed,
    warehouse_name,
    user_name,
    DATEDIFF('minute', run_ended_at, CURRENT_TIMESTAMP())    AS minutes_since_last_run
FROM DBT_RUN_LOG
ORDER BY run_started_at DESC
LIMIT 10;


-- ═══════════════════════════════════════════════════════════════
-- 2. ERROR ANALYSIS
-- ═══════════════════════════════════════════════════════════════

-- 2a. Error frequency by model (which models fail most?)
CREATE OR REPLACE VIEW V_ERROR_FREQUENCY_BY_MODEL AS
WITH error_messages AS (
    SELECT
        model_name,
        schema_name,
        materialization,
        started_at,
        COALESCE(error_message, 'unknown')                   AS error_msg,
        ROW_NUMBER() OVER (
            PARTITION BY model_name, schema_name, materialization, error_message
            ORDER BY started_at DESC
        )                                                    AS dedup_rn
    FROM DBT_MODEL_LOG
    WHERE status IN ('FAIL', 'ERROR')
      AND started_at >= DATEADD('day', -30, CURRENT_DATE())
)
SELECT
    model_name,
    schema_name,
    materialization,
    COUNT(*)                                                 AS total_failures,
    MIN(started_at)                                          AS first_failure_at,
    MAX(started_at)                                          AS last_failure_at,
    DATEDIFF('hour', MAX(started_at), CURRENT_TIMESTAMP())   AS hours_since_last_failure,
    LISTAGG(error_msg, ' | ')
        WITHIN GROUP (ORDER BY started_at DESC)              AS recent_errors
FROM error_messages
WHERE dedup_rn = 1
GROUP BY model_name, schema_name, materialization
ORDER BY total_failures DESC;

-- 2b. Error timeline (when do errors cluster?)
CREATE OR REPLACE VIEW V_ERROR_TIMELINE AS
SELECT
    DATE(m.started_at)                                       AS error_date,
    HOUR(m.started_at)                                       AS error_hour,
    COUNT(*)                                                 AS error_count,
    LISTAGG(DISTINCT m.model_name, ', ')
        WITHIN GROUP (ORDER BY m.model_name)                 AS affected_models,
    r.environment
FROM DBT_MODEL_LOG m
LEFT JOIN DBT_RUN_LOG r ON m.run_id = r.run_id
WHERE m.status IN ('FAIL', 'ERROR')
  AND m.started_at >= DATEADD('day', -14, CURRENT_DATE())
GROUP BY 1, 2, r.environment
ORDER BY 1 DESC, 2;

-- 2c. Consecutive failures (models stuck in failure)
CREATE OR REPLACE VIEW V_CONSECUTIVE_FAILURES AS
WITH ranked AS (
    SELECT
        model_name,
        status,
        started_at,
        ROW_NUMBER() OVER (PARTITION BY model_name ORDER BY started_at DESC) AS rn
    FROM DBT_MODEL_LOG
    WHERE started_at >= DATEADD('day', -7, CURRENT_DATE())
)
SELECT
    model_name,
    COUNT(*)                                                 AS consecutive_failures,
    MIN(started_at)                                          AS failure_streak_start,
    MAX(started_at)                                          AS failure_streak_end
FROM ranked
WHERE status IN ('FAIL', 'ERROR')
  AND rn <= 10
GROUP BY model_name
HAVING consecutive_failures >= 2
ORDER BY consecutive_failures DESC;


-- ═══════════════════════════════════════════════════════════════
-- 3. PERFORMANCE ANALYSIS
-- ═══════════════════════════════════════════════════════════════

-- 3a. Slowest models (top 20 by average row count per run)
CREATE OR REPLACE VIEW V_MODEL_PERFORMANCE AS
SELECT
    model_name,
    materialization,
    incremental_strategy,
    COUNT(*)                                                 AS total_runs,
    ROUND(AVG(rows_affected), 0)                             AS avg_rows,
    ROUND(MAX(rows_affected), 0)                             AS max_rows,
    ROUND(STDDEV(rows_affected), 0)                          AS stddev_rows,
    SUM(IFF(status = 'SUCCESS', 1, 0))                       AS success_count,
    SUM(IFF(status IN ('FAIL','ERROR'), 1, 0))               AS fail_count,
    MAX(started_at)                                          AS last_run_at
FROM DBT_MODEL_LOG
WHERE started_at >= DATEADD('day', -14, CURRENT_DATE())
GROUP BY model_name, materialization, incremental_strategy
ORDER BY avg_rows DESC
LIMIT 20;

-- 3b. Row count trend per model (detect data volume changes)
CREATE OR REPLACE VIEW V_ROW_COUNT_TREND AS
WITH daily_rows AS (
    SELECT
        model_name,
        DATE(started_at)                                     AS run_date,
        AVG(rows_affected)                                   AS avg_rows,
        MAX(rows_affected)                                   AS max_rows,
        MIN(rows_affected)                                   AS min_rows
    FROM DBT_MODEL_LOG
    WHERE rows_affected IS NOT NULL
      AND started_at >= DATEADD('day', -14, CURRENT_DATE())
    GROUP BY model_name, DATE(started_at)
)
SELECT
    model_name,
    run_date,
    avg_rows,
    max_rows,
    min_rows,
    LAG(avg_rows) OVER (
        PARTITION BY model_name ORDER BY run_date
    )                                                        AS prev_day_avg_rows,
    ROUND(
        (avg_rows - LAG(avg_rows) OVER (
            PARTITION BY model_name ORDER BY run_date
        )) * 100.0
        / NULLIF(LAG(avg_rows) OVER (
            PARTITION BY model_name ORDER BY run_date
        ), 0), 1
    )                                                        AS pct_change
FROM daily_rows
ORDER BY model_name, run_date DESC;

-- 3c. Incremental model efficiency (rows processed per run)
CREATE OR REPLACE VIEW V_INCREMENTAL_EFFICIENCY AS
SELECT
    model_name,
    incremental_strategy,
    COUNT(*)                                                 AS total_runs,
    ROUND(AVG(rows_affected), 0)                             AS avg_rows_per_run,
    ROUND(MEDIAN(rows_affected), 0)                          AS median_rows_per_run,
    ROUND(MAX(rows_affected), 0)                             AS max_rows_per_run,
    SUM(rows_affected)                                       AS total_rows_processed,
    MIN(started_at)                                          AS first_run,
    MAX(started_at)                                          AS last_run
FROM DBT_MODEL_LOG
WHERE is_incremental = TRUE
  AND status = 'SUCCESS'
  AND started_at >= DATEADD('day', -14, CURRENT_DATE())
GROUP BY model_name, incremental_strategy
ORDER BY avg_rows_per_run DESC;


-- ═══════════════════════════════════════════════════════════════
-- 4. COST ANALYSIS (Snowflake credits via QUERY_HISTORY)
-- ═══════════════════════════════════════════════════════════════

-- 4a. Credit consumption by dbt query_tag (requires ACCOUNTADMIN or MONITOR)
CREATE OR REPLACE VIEW V_COST_BY_QUERY_TAG AS
SELECT
    query_tag,
    DATE(start_time)                                         AS query_date,
    COUNT(*)                                                 AS query_count,
    ROUND(SUM(total_elapsed_time) / 1000.0, 2)               AS total_elapsed_sec,
    ROUND(AVG(total_elapsed_time) / 1000.0, 2)               AS avg_elapsed_sec,
    ROUND(SUM(credits_used_cloud_services), 4)               AS cloud_credits,
    SUM(bytes_scanned)                                       AS total_bytes_scanned,
    ROUND(SUM(bytes_scanned) / POWER(1024, 3), 3)            AS total_gb_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag LIKE 'dbt_%newsletter%'
  AND start_time >= DATEADD('day', -14, CURRENT_DATE())
GROUP BY query_tag, DATE(start_time)
ORDER BY total_elapsed_sec DESC;

-- 4b. Most expensive dbt queries (top 20)
CREATE OR REPLACE VIEW V_MOST_EXPENSIVE_QUERIES AS
SELECT
    query_tag,
    query_id,
    query_text,
    warehouse_name,
    DATE(start_time)                                         AS query_date,
    ROUND(total_elapsed_time / 1000.0, 2)                    AS elapsed_sec,
    ROUND(credits_used_cloud_services, 6)                    AS cloud_credits,
    ROUND(bytes_scanned / POWER(1024, 3), 3)                 AS gb_scanned,
    rows_produced,
    execution_status
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag LIKE 'dbt_%newsletter%'
  AND start_time >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY total_elapsed_time DESC
LIMIT 20;

-- 4c. Daily credit trend for dbt workloads
CREATE OR REPLACE VIEW V_DAILY_CREDIT_TREND AS
SELECT
    DATE(start_time)                                         AS query_date,
    COUNT(*)                                                 AS total_queries,
    ROUND(SUM(total_elapsed_time) / 1000.0, 2)               AS total_elapsed_sec,
    ROUND(SUM(credits_used_cloud_services), 4)               AS total_cloud_credits,
    ROUND(SUM(bytes_scanned) / POWER(1024, 3), 3)            AS total_gb_scanned,
    ROUND(AVG(total_elapsed_time) / 1000.0, 2)               AS avg_query_sec
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag LIKE 'dbt_%newsletter%'
  AND start_time >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1
ORDER BY 1 DESC;

-- 4d. Cost breakdown by model layer
CREATE OR REPLACE VIEW V_COST_BY_LAYER AS
SELECT
    CASE
        WHEN query_tag LIKE '%_stg_%'  THEN 'staging'
        WHEN query_tag LIKE '%_fct_%'  THEN 'marts'
        WHEN query_tag LIKE '%snap_%'  THEN 'snapshots'
        ELSE 'other'
    END                                                      AS layer,
    COUNT(*)                                                 AS query_count,
    ROUND(SUM(total_elapsed_time) / 1000.0, 2)               AS total_elapsed_sec,
    ROUND(SUM(credits_used_cloud_services), 4)               AS total_cloud_credits,
    ROUND(SUM(bytes_scanned) / POWER(1024, 3), 3)            AS total_gb_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag LIKE 'dbt_%newsletter%'
  AND start_time >= DATEADD('day', -14, CURRENT_DATE())
GROUP BY 1
ORDER BY total_cloud_credits DESC;


-- ═══════════════════════════════════════════════════════════════
-- 5. LOG ANALYSIS
-- ═══════════════════════════════════════════════════════════════

-- 5a. Run log analysis (run patterns)
CREATE OR REPLACE VIEW V_RUN_LOG_ANALYSIS AS
SELECT
    run_id,
    project_name,
    environment,
    run_status,
    run_started_at,
    run_ended_at,
    run_duration_seconds,
    models_run,
    models_success,
    models_failed,
    warehouse_name,
    user_name,
    CASE
        WHEN models_failed > 0 THEN 'NEEDS_ATTENTION'
        WHEN run_duration_seconds > 300 THEN 'SLOW_RUN'
        ELSE 'HEALTHY'
    END                                                      AS health_indicator,
    CASE
        WHEN models_run > 0
        THEN ROUND(models_success * 100.0 / models_run, 1)
        ELSE 0
    END                                                      AS model_success_rate_pct
FROM DBT_RUN_LOG
WHERE run_started_at >= DATEADD('day', -14, CURRENT_DATE())
ORDER BY run_started_at DESC;

-- 5b. Model execution log (detailed per-model tracking)
CREATE OR REPLACE VIEW V_MODEL_LOG_DETAIL AS
SELECT
    m.model_name,
    m.materialization,
    m.schema_name,
    m.status,
    m.is_incremental,
    m.incremental_strategy,
    m.rows_affected,
    m.started_at,
    m.batch_id,
    r.run_status                                             AS parent_run_status,
    r.environment,
    r.user_name,
    r.warehouse_name
FROM DBT_MODEL_LOG m
LEFT JOIN DBT_RUN_LOG r ON m.run_id = r.run_id
WHERE m.started_at >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY m.started_at DESC;


-- ═══════════════════════════════════════════════════════════════
-- 6. TESTING METRICS
-- ═══════════════════════════════════════════════════════════════

-- 6a. Test results from Snowflake query history
-- dbt tests compile to "select count(*) as failures..." queries.
-- Tests don't inherit model query_tags, so we match by SQL pattern
-- and target schema references.
CREATE OR REPLACE VIEW V_TEST_RESULTS AS
SELECT
    COALESCE(query_tag, 'no_tag')                            AS query_tag,
    DATE(start_time)                                         AS test_date,
    execution_status,
    COUNT(*)                                                 AS test_count,
    ROUND(AVG(total_elapsed_time) / 1000.0, 2)               AS avg_test_sec,
    SUM(rows_produced)                                       AS total_rows_returned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE (
        LOWER(query_text) LIKE '%select count(*) as failures%'
        OR LOWER(query_text) LIKE '%select count(*) as validation_errors%'
        OR (LOWER(query_text) LIKE '%select count(%)%'
            AND LOWER(query_text) LIKE '%simpplr_dbt_%')
    )
  AND LOWER(query_text) NOT LIKE '%insert%'
  AND LOWER(query_text) NOT LIKE '%create%'
  AND LOWER(query_text) NOT LIKE '%information_schema%'
  AND start_time >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY COALESCE(query_tag, 'no_tag'), DATE(start_time), execution_status
ORDER BY test_date DESC;

-- 6b. Test pass/fail summary by day
CREATE OR REPLACE VIEW V_TEST_DAILY_SUMMARY AS
SELECT
    DATE(start_time)                                         AS test_date,
    SUM(IFF(execution_status = 'SUCCESS' AND rows_produced = 0, 1, 0)) AS tests_passed,
    SUM(IFF(execution_status = 'SUCCESS' AND rows_produced > 0, 1, 0)) AS tests_failed,
    SUM(IFF(execution_status != 'SUCCESS', 1, 0))            AS tests_errored,
    tests_passed + tests_failed + tests_errored              AS total_tests,
    ROUND(tests_passed * 100.0
          / NULLIF(total_tests, 0), 1)                       AS pass_rate_pct
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE (
        LOWER(query_text) LIKE '%select count(*) as failures%'
        OR LOWER(query_text) LIKE '%select count(*) as validation_errors%'
        OR (LOWER(query_text) LIKE '%select count(%)%'
            AND LOWER(query_text) LIKE '%simpplr_dbt_%')
    )
  AND LOWER(query_text) NOT LIKE '%insert%'
  AND LOWER(query_text) NOT LIKE '%create%'
  AND LOWER(query_text) NOT LIKE '%information_schema%'
  AND start_time >= DATEADD('day', -14, CURRENT_DATE())
GROUP BY 1
ORDER BY 1 DESC;


-- ═══════════════════════════════════════════════════════════════
-- 7. DATA FRESHNESS & QUALITY
-- ═══════════════════════════════════════════════════════════════

-- 7a. Data freshness per mart table
CREATE OR REPLACE VIEW V_DATA_FRESHNESS AS
SELECT
    'fct_newsletter'             AS model_name,
    MAX(dbt_loaded_at)           AS last_loaded_at,
    COUNT(*)                     AS total_rows,
    DATEDIFF('minute',
        MAX(dbt_loaded_at),
        CURRENT_TIMESTAMP())     AS minutes_since_last_load
FROM SIMPPLR_DBT_DEV.SIMPPLR_DBT_MARTS.FCT_NEWSLETTER

UNION ALL

SELECT
    'fct_newsletter_interaction',
    MAX(dbt_loaded_at),
    COUNT(*),
    DATEDIFF('minute', MAX(dbt_loaded_at), CURRENT_TIMESTAMP())
FROM SIMPPLR_DBT_DEV.SIMPPLR_DBT_MARTS.FCT_NEWSLETTER_INTERACTION

UNION ALL

SELECT
    'fct_newsletter_category',
    MAX(dbt_loaded_at),
    COUNT(*),
    DATEDIFF('minute', MAX(dbt_loaded_at), CURRENT_TIMESTAMP())
FROM SIMPPLR_DBT_DEV.SIMPPLR_DBT_MARTS.FCT_NEWSLETTER_CATEGORY
ORDER BY minutes_since_last_load DESC;

-- 7b. Data quality overview (null rates on key columns)
CREATE OR REPLACE VIEW V_DATA_QUALITY_NEWSLETTER AS
SELECT
    'fct_newsletter'                                         AS model_name,
    COUNT(*)                                                 AS total_rows,
    ROUND(SUM(IFF(tenant_code = 'N/A', 1, 0)) * 100.0
          / NULLIF(COUNT(*), 0), 2)                          AS pct_unknown_tenant,
    ROUND(SUM(IFF(status_code = 'NLS000', 1, 0)) * 100.0
          / NULLIF(COUNT(*), 0), 2)                          AS pct_unmapped_status,
    ROUND(SUM(IFF(recipient_type_code = 'NLRT000', 1, 0)) * 100.0
          / NULLIF(COUNT(*), 0), 2)                          AS pct_unmapped_recipient_type,
    ROUND(SUM(IFF(data_source_code IS NULL, 1, 0)) * 100.0
          / NULLIF(COUNT(*), 0), 2)                          AS pct_null_data_source,
    ROUND(SUM(IFF(hash_value IS NULL, 1, 0)) * 100.0
          / NULLIF(COUNT(*), 0), 2)                          AS pct_null_hash,
    ROUND(SUM(IFF(is_deleted, 1, 0)) * 100.0
          / NULLIF(COUNT(*), 0), 2)                          AS pct_deleted
FROM SIMPPLR_DBT_DEV.SIMPPLR_DBT_MARTS.FCT_NEWSLETTER;


-- ═══════════════════════════════════════════════════════════════
-- 8. OPERATIONAL ALERTS (queries to drive Snowflake alerts)
-- ═══════════════════════════════════════════════════════════════

-- 8a. Stale data alert (no load in last N hours)
-- Use with: CREATE ALERT ... IF (EXISTS (SELECT 1 FROM V_STALE_DATA_ALERT))
CREATE OR REPLACE VIEW V_STALE_DATA_ALERT AS
SELECT
    model_name,
    last_loaded_at,
    minutes_since_last_load
FROM V_DATA_FRESHNESS
WHERE minutes_since_last_load > 1440;  -- > 24 hours

-- 8b. Failed run alert
CREATE OR REPLACE VIEW V_FAILED_RUN_ALERT AS
SELECT
    run_id,
    run_status,
    run_started_at,
    models_failed,
    user_name
FROM DBT_RUN_LOG
WHERE run_status = 'FAILED'
  AND run_started_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP());

-- 8c. Row count anomaly alert (sudden drop > 50%)
CREATE OR REPLACE VIEW V_ROW_COUNT_ANOMALY_ALERT AS
SELECT
    model_name,
    run_date,
    avg_rows,
    prev_day_avg_rows,
    pct_change
FROM V_ROW_COUNT_TREND
WHERE pct_change < -50
  AND prev_day_avg_rows > 0
  AND run_date >= DATEADD('day', -3, CURRENT_DATE());
