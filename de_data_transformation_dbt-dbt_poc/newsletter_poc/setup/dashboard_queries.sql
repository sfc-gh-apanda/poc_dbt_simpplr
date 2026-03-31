-- =============================================================
-- SIMPPLR DBT PoC — Snowsight Dashboard Tile Queries
-- =============================================================
-- Prerequisites: Run audit_setup.sql, then monitoring_queries.sql
-- Usage: Copy each query into a Snowsight Dashboard tile.
--        Recommended chart type noted above each query.
-- =============================================================

USE DATABASE COMMON_TENANT_DEV;
USE SCHEMA DBT_EXECUTION_RUN_STATS;


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  SECTION A: EXECUTIVE OVERVIEW                               ║
-- ║  One-glance health for the entire dbt pipeline               ║
-- ╚═══════════════════════════════════════════════════════════════╝

-- ─────────────────────────────────────────────────────────────────
-- A1. Pipeline Health Scorecard  [CHART: Scorecard / KPI tiles]
-- Show as 4 separate scorecard tiles
-- ─────────────────────────────────────────────────────────────────

-- A1a. Last Run Status
SELECT
    run_status                                               AS "Status",
    run_duration_seconds                                     AS "Duration (sec)",
    models_run                                               AS "Models",
    models_failed                                            AS "Failures",
    TO_CHAR(run_started_at, 'YYYY-MM-DD HH24:MI')           AS "Started At"
FROM DBT_RUN_LOG
WHERE run_status != 'RUNNING'
ORDER BY run_started_at DESC
LIMIT 1;

-- A1b. Overall Success Rate (last 30 days)
SELECT
    ROUND(
        SUM(IFF(run_status = 'SUCCESS', 1, 0)) * 100.0
        / NULLIF(COUNT(*), 0), 1
    )                                                        AS "Success Rate %"
FROM DBT_RUN_LOG
WHERE run_started_at >= DATEADD('day', -30, CURRENT_DATE())
  AND run_status != 'RUNNING';

-- A1c. Total Models Processed Today
SELECT
    COALESCE(SUM(models_run), 0)                             AS "Models Today",
    COALESCE(SUM(models_success), 0)                         AS "Succeeded",
    COALESCE(SUM(models_failed), 0)                          AS "Failed"
FROM DBT_RUN_LOG
WHERE DATE(run_started_at) = CURRENT_DATE()
  AND run_status != 'RUNNING';

-- A1d. Minutes Since Last Successful Run
SELECT
    DATEDIFF('minute', MAX(run_ended_at), CURRENT_TIMESTAMP()) AS "Minutes Since Last Success"
FROM DBT_RUN_LOG
WHERE run_status = 'SUCCESS';


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  SECTION B: AUTOMATED TESTING (Client Req #1)                ║
-- ║  In-pipeline test coverage, pass rates, failure details      ║
-- ╚═══════════════════════════════════════════════════════════════╝

-- ─────────────────────────────────────────────────────────────────
-- B1. Test Pass Rate Trend  [CHART: Line chart, X=date, Y=rate%]
-- Uses ACCOUNT_USAGE.QUERY_HISTORY for up to 365 days of history
-- (note: ~45 min latency). Adjust DATEADD lookback as needed.
-- ─────────────────────────────────────────────────────────────────
SELECT
    DATE(start_time)                                         AS "Test Date",
    COUNT(*)                                                 AS "Total Tests",
    SUM(IFF(execution_status = 'SUCCESS'
        AND rows_produced = 0, 1, 0))                        AS "Passed",
    SUM(IFF(execution_status = 'SUCCESS'
        AND rows_produced > 0, 1, 0))                        AS "Failed",
    SUM(IFF(execution_status != 'SUCCESS', 1, 0))            AS "Errored",
    ROUND(
        SUM(IFF(execution_status = 'SUCCESS'
            AND rows_produced = 0, 1, 0)) * 100.0
        / NULLIF(COUNT(*), 0), 1
    )                                                        AS "Pass Rate %"
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  AND (
        LOWER(query_text) LIKE '%select count(*) as failures%'
        OR LOWER(query_text) LIKE '%select count(*) as validation_errors%'
        OR (LOWER(query_text) LIKE '%select count(%)%'
            AND LOWER(query_text) LIKE '%simpplr_dbt_%')
    )
  AND LOWER(query_text) NOT LIKE '%insert%'
  AND LOWER(query_text) NOT LIKE '%create%'
  AND LOWER(query_text) NOT LIKE '%information_schema%'
  AND LOWER(query_text) NOT LIKE '%account_usage%'
GROUP BY 1
ORDER BY 1 DESC;

-- ─────────────────────────────────────────────────────────────────
-- B2. Test Failures Detail  [CHART: Table]
-- Shows which tests failed and what data they found
-- ─────────────────────────────────────────────────────────────────
SELECT
    COALESCE(query_tag, 'no_tag')                            AS "Query Tag",
    REGEXP_SUBSTR(query_text, 'from\\s+(\\S+)', 1, 1, 'i', 1) AS "Tested Model",
    rows_produced                                            AS "Failing Rows",
    ROUND(total_elapsed_time / 1000.0, 2)                    AS "Duration (sec)",
    TO_CHAR(start_time, 'YYYY-MM-DD HH24:MI')               AS "Run At",
    LEFT(query_text, 200)                                    AS "Test Query (preview)"
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  AND (
        LOWER(query_text) LIKE '%select count(*) as failures%'
        OR LOWER(query_text) LIKE '%select count(*) as validation_errors%'
        OR (LOWER(query_text) LIKE '%select count(%)%'
            AND LOWER(query_text) LIKE '%simpplr_dbt_%')
    )
  AND LOWER(query_text) NOT LIKE '%insert%'
  AND LOWER(query_text) NOT LIKE '%account_usage%'
  AND execution_status = 'SUCCESS'
  AND rows_produced > 0
ORDER BY start_time DESC
LIMIT 50;

-- ─────────────────────────────────────────────────────────────────
-- B3. Tests Per Model Layer  [CHART: Bar chart, X=layer, Y=count]
-- Classifies tests by which schema/table they query against
-- ─────────────────────────────────────────────────────────────────
SELECT
    CASE
        WHEN LOWER(query_text) LIKE '%simpplr_dbt_staging%'    THEN '1. Staging'
        WHEN LOWER(query_text) LIKE '%simpplr_dbt_marts%'      THEN '3. Marts'
        WHEN LOWER(query_text) LIKE '%simpplr_dbt_snapshots%'  THEN '4. Snapshots'
        WHEN LOWER(query_text) LIKE '%simpplr_dbt_sources%'    THEN '0. Sources'
        WHEN LOWER(query_text) LIKE '%simpplr_dbt_seeds%'      THEN '0. Seeds'
        ELSE '5. Other'
    END                                                      AS "Layer",
    COUNT(*)                                                 AS "Total Tests",
    SUM(IFF(execution_status = 'SUCCESS'
        AND rows_produced = 0, 1, 0))                        AS "Passed",
    SUM(IFF(execution_status = 'SUCCESS'
        AND rows_produced > 0, 1, 0))                        AS "Failed"
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  AND (
        LOWER(query_text) LIKE '%select count(*) as failures%'
        OR LOWER(query_text) LIKE '%select count(*) as validation_errors%'
        OR (LOWER(query_text) LIKE '%select count(%)%'
            AND LOWER(query_text) LIKE '%simpplr_dbt_%')
    )
  AND LOWER(query_text) NOT LIKE '%insert%'
  AND LOWER(query_text) NOT LIKE '%account_usage%'
GROUP BY 1
ORDER BY 1;

-- ─────────────────────────────────────────────────────────────────
-- B4. Contract Enforcement Status  [CHART: Table]
-- Shows which models have enforced contracts (from query patterns)
-- ─────────────────────────────────────────────────────────────────
SELECT
    m.model_name                                             AS "Model",
    m.schema_name                                            AS "Schema",
    m.materialization                                        AS "Materialization",
    IFF(m.materialization = 'incremental', 'YES', 'NO')      AS "Contract Enforced",
    IFF(m.is_incremental, 'YES', 'NO')                       AS "Incremental",
    m.incremental_strategy                                   AS "Strategy",
    COUNT(*)                                                 AS "Total Runs",
    SUM(IFF(m.status = 'SUCCESS', 1, 0))                     AS "Successes",
    SUM(IFF(m.status IN ('FAIL','ERROR'), 1, 0))             AS "Failures"
FROM DBT_MODEL_LOG m
WHERE m.started_at >= DATEADD('day', -14, CURRENT_DATE())
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY m.model_name;


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  SECTION C: DEVELOPER PRODUCTIVITY (Client Req #2)           ║
-- ║  Code reusability metrics, model complexity, build speed     ║
-- ╚═══════════════════════════════════════════════════════════════╝

-- ─────────────────────────────────────────────────────────────────
-- C1. Project Composition  [CHART: Stacked bar or pie]
-- Shows model count by layer and materialization
-- ─────────────────────────────────────────────────────────────────
SELECT
    CASE
        WHEN schema_name ILIKE '%staging%'       THEN 'Staging'
        WHEN schema_name ILIKE '%intermediate%'  THEN 'Intermediate'
        WHEN schema_name ILIKE '%marts%'         THEN 'Marts'
        WHEN schema_name ILIKE '%snapshot%'      THEN 'Snapshots'
        WHEN schema_name ILIKE '%seeds%'         THEN 'Seeds'
        ELSE 'Other'
    END                                                      AS "Layer",
    materialization                                          AS "Materialization",
    COUNT(DISTINCT model_name)                               AS "Model Count"
FROM DBT_MODEL_LOG
WHERE started_at >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1;

-- ─────────────────────────────────────────────────────────────────
-- C2. Build Time by Layer  [CHART: Horizontal bar]
-- Shows where build time is spent
-- ─────────────────────────────────────────────────────────────────
SELECT
    CASE
        WHEN query_tag LIKE '%_stg_%'     THEN '1. Staging'
        WHEN query_tag LIKE '%_fct_%'     THEN '2. Marts'
        WHEN query_tag LIKE '%snap_%'     THEN '3. Snapshots'
        WHEN query_tag LIKE '%seed%'      THEN '0. Seeds'
        ELSE '4. Other (tests/hooks)'
    END                                                      AS "Layer",
    COUNT(*)                                                 AS "Queries",
    ROUND(SUM(total_elapsed_time) / 1000.0, 2)               AS "Total Time (sec)",
    ROUND(AVG(total_elapsed_time) / 1000.0, 2)               AS "Avg Time (sec)",
    ROUND(SUM(total_elapsed_time) * 100.0
        / NULLIF(SUM(SUM(total_elapsed_time)) OVER (), 0), 1) AS "% of Total"
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag LIKE 'dbt_%newsletter%'
  AND start_time >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY 1
ORDER BY 1;

-- ─────────────────────────────────────────────────────────────────
-- C3. Macro/Reuse Indicator  [CHART: Scorecard / KPI]
-- Demonstrates code reusability via shared patterns
-- ─────────────────────────────────────────────────────────────────
SELECT
    COUNT(DISTINCT model_name)                               AS "Total Models",
    SUM(IFF(is_incremental, 1, 0))                           AS "Incremental (shared pattern)",
    COUNT(DISTINCT model_name) -
        SUM(IFF(is_incremental, 1, 0))                       AS "Non-Incremental",
    COUNT(DISTINCT incremental_strategy)                      AS "Strategies Used",
    COUNT(DISTINCT schema_name)                               AS "Schemas Used"
FROM DBT_MODEL_LOG
WHERE started_at = (SELECT MAX(started_at) FROM DBT_MODEL_LOG);


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  SECTION D: FAILURE RECOVERY & COST (Client Req #4)          ║
-- ║  Recovery time, retry effectiveness, credit consumption      ║
-- ╚═══════════════════════════════════════════════════════════════╝

-- ─────────────────────────────────────────────────────────────────
-- D1. Daily Credit Consumption  [CHART: Line chart, X=date]
-- ─────────────────────────────────────────────────────────────────
SELECT
    DATE(start_time)                                         AS "Date",
    COUNT(*)                                                 AS "Queries",
    ROUND(SUM(total_elapsed_time) / 1000.0, 2)               AS "Total Time (sec)",
    ROUND(SUM(credits_used_cloud_services), 4)               AS "Cloud Credits",
    ROUND(SUM(bytes_scanned) / POWER(1024, 3), 3)            AS "GB Scanned"
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag LIKE 'dbt_%newsletter%'
  AND start_time >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1
ORDER BY 1 DESC;

-- ─────────────────────────────────────────────────────────────────
-- D2. Cost by Model Layer  [CHART: Donut / pie chart]
-- ─────────────────────────────────────────────────────────────────
SELECT
    CASE
        WHEN query_tag LIKE '%_stg_%'     THEN 'Staging'
        WHEN query_tag LIKE '%_fct_%'     THEN 'Marts'
        WHEN query_tag LIKE '%snap_%'     THEN 'Snapshots'
        WHEN query_tag LIKE '%seed%'      THEN 'Seeds'
        ELSE 'Other (tests/hooks)'
    END                                                      AS "Layer",
    COUNT(*)                                                 AS "Query Count",
    ROUND(SUM(total_elapsed_time) / 1000.0, 2)               AS "Total Elapsed (sec)",
    ROUND(SUM(credits_used_cloud_services), 6)               AS "Cloud Credits",
    ROUND(SUM(bytes_scanned) / POWER(1024, 3), 3)            AS "GB Scanned"
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag LIKE 'dbt_%newsletter%'
  AND start_time >= DATEADD('day', -14, CURRENT_DATE())
GROUP BY 1
ORDER BY "Cloud Credits" DESC;

-- ─────────────────────────────────────────────────────────────────
-- D3. Top 15 Most Expensive Queries  [CHART: Table]
-- ─────────────────────────────────────────────────────────────────
SELECT
    query_tag                                                AS "Tag",
    query_id                                                 AS "Query ID",
    warehouse_name                                           AS "Warehouse",
    ROUND(total_elapsed_time / 1000.0, 2)                    AS "Elapsed (sec)",
    ROUND(credits_used_cloud_services, 6)                    AS "Credits",
    ROUND(bytes_scanned / POWER(1024, 3), 4)                 AS "GB Scanned",
    rows_produced                                            AS "Rows",
    execution_status                                         AS "Status",
    TO_CHAR(start_time, 'YYYY-MM-DD HH24:MI')               AS "Started At",
    LEFT(query_text, 150)                                    AS "SQL Preview"
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag LIKE 'dbt_%newsletter%'
  AND start_time >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY total_elapsed_time DESC
LIMIT 15;

-- ─────────────────────────────────────────────────────────────────
-- D4. Failed Runs & Recovery Time  [CHART: Table]
-- Shows failures and time to next successful run (recovery speed)
-- ─────────────────────────────────────────────────────────────────
WITH run_sequence AS (
    SELECT
        run_id,
        run_status,
        run_started_at,
        run_ended_at,
        run_duration_seconds,
        models_failed,
        LEAD(run_started_at) OVER (ORDER BY run_started_at)  AS next_run_start,
        LEAD(run_status) OVER (ORDER BY run_started_at)      AS next_run_status
    FROM DBT_RUN_LOG
    WHERE run_status != 'RUNNING'
)
SELECT
    run_id                                                   AS "Failed Run ID",
    TO_CHAR(run_started_at, 'YYYY-MM-DD HH24:MI')           AS "Failed At",
    models_failed                                            AS "Models Failed",
    run_duration_seconds                                     AS "Duration (sec)",
    next_run_status                                          AS "Next Run Status",
    ROUND(DATEDIFF('second', run_ended_at, next_run_start) / 60.0, 1)
                                                             AS "Recovery Time (min)",
    IFF(next_run_status = 'SUCCESS', 'RECOVERED', 'STILL FAILING')
                                                             AS "Recovery Status"
FROM run_sequence
WHERE run_status = 'FAILED'
  AND run_started_at >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY run_started_at DESC;

-- ─────────────────────────────────────────────────────────────────
-- D5. Incremental vs Full Refresh Cost  [CHART: Grouped bar]
-- Demonstrates cost savings from incremental processing
-- ─────────────────────────────────────────────────────────────────
SELECT
    m.model_name                                             AS "Model",
    IFF(m.is_incremental, 'Incremental', 'Full Refresh')     AS "Run Type",
    COUNT(*)                                                 AS "Runs",
    ROUND(AVG(m.rows_affected), 0)                           AS "Avg Rows",
    ROUND(AVG(q.total_elapsed_time) / 1000.0, 2)             AS "Avg Query Time (sec)",
    ROUND(AVG(q.bytes_scanned) / POWER(1024, 2), 2)          AS "Avg MB Scanned"
FROM DBT_MODEL_LOG m
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY q
    ON q.query_tag = CONCAT('dbt_', REPLACE(m.model_name, 'fct_', 'fct_'))
   AND DATE(q.start_time) = DATE(m.started_at)
WHERE m.materialization = 'incremental'
  AND m.status = 'SUCCESS'
  AND m.started_at >= DATEADD('day', -14, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1, 2;

-- ─────────────────────────────────────────────────────────────────
-- D6. Warehouse Credit Usage Over Time  [CHART: Area chart]
-- ─────────────────────────────────────────────────────────────────
SELECT
    DATE(start_time)                                         AS "Date",
    warehouse_name                                           AS "Warehouse",
    COUNT(*)                                                 AS "Query Count",
    ROUND(SUM(credits_used_cloud_services), 4)               AS "Credits Used",
    ROUND(SUM(bytes_scanned) / POWER(1024, 3), 3)            AS "GB Scanned",
    ROUND(SUM(total_elapsed_time) / 1000.0 / 60.0, 2)       AS "Total Minutes"
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag LIKE 'dbt_%newsletter%'
  AND start_time >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1 DESC, 2;


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  SECTION E: PERFORMANCE ANALYSIS (Client Req #5)             ║
-- ║  Execution trends, model timing, optimization opportunities  ║
-- ╚═══════════════════════════════════════════════════════════════╝

-- ─────────────────────────────────────────────────────────────────
-- E1. Pipeline Duration Trend  [CHART: Line chart with rolling avg]
-- ─────────────────────────────────────────────────────────────────
SELECT
    DATE(run_started_at)                                     AS "Run Date",
    run_duration_seconds                                     AS "Duration (sec)",
    ROUND(AVG(run_duration_seconds) OVER (
        ORDER BY run_started_at
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2)                                                    AS "7-Run Rolling Avg (sec)",
    models_run                                               AS "Models",
    run_id                                                   AS "Run ID"
FROM DBT_RUN_LOG
WHERE run_started_at >= DATEADD('day', -30, CURRENT_DATE())
  AND run_status != 'RUNNING'
ORDER BY run_started_at;

-- ─────────────────────────────────────────────────────────────────
-- E2. Model-Level Performance  [CHART: Horizontal bar]
-- Average execution time per model
-- ─────────────────────────────────────────────────────────────────
SELECT
    query_tag                                                AS "Model Tag",
    COUNT(*)                                                 AS "Executions",
    ROUND(AVG(total_elapsed_time) / 1000.0, 2)               AS "Avg Time (sec)",
    ROUND(MAX(total_elapsed_time) / 1000.0, 2)               AS "Max Time (sec)",
    ROUND(MIN(total_elapsed_time) / 1000.0, 2)               AS "Min Time (sec)",
    ROUND(STDDEV(total_elapsed_time) / 1000.0, 2)            AS "Stddev (sec)",
    ROUND(AVG(bytes_scanned) / POWER(1024, 2), 2)            AS "Avg MB Scanned"
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag LIKE 'dbt_%newsletter%'
  AND query_tag NOT LIKE '%test%'
  AND execution_status = 'SUCCESS'
  AND start_time >= DATEADD('day', -14, CURRENT_DATE())
GROUP BY 1
ORDER BY "Avg Time (sec)" DESC;

-- ─────────────────────────────────────────────────────────────────
-- E3. Incremental Efficiency  [CHART: Table / KPI]
-- Shows rows processed per incremental run vs total table size
-- ─────────────────────────────────────────────────────────────────
SELECT
    m.model_name                                             AS "Model",
    m.incremental_strategy                                   AS "Strategy",
    COUNT(*)                                                 AS "Total Runs",
    ROUND(AVG(m.rows_affected), 0)                           AS "Avg Delta Rows",
    ROUND(MEDIAN(m.rows_affected), 0)                        AS "Median Delta Rows",
    ROUND(MAX(m.rows_affected), 0)                           AS "Max Delta Rows",
    SUM(m.rows_affected)                                     AS "Cumulative Rows Processed"
FROM DBT_MODEL_LOG m
WHERE m.is_incremental = TRUE
  AND m.status = 'SUCCESS'
  AND m.started_at >= DATEADD('day', -14, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY "Avg Delta Rows" DESC;

-- ─────────────────────────────────────────────────────────────────
-- E4. Row Count Trends  [CHART: Multi-series line chart]
-- Detect data volume changes per model
-- ─────────────────────────────────────────────────────────────────
WITH daily_rows AS (
    SELECT
        model_name,
        DATE(started_at)                                     AS run_date,
        AVG(rows_affected)                                   AS avg_rows,
        MAX(rows_affected)                                   AS max_rows,
        MIN(rows_affected)                                   AS min_rows
    FROM DBT_MODEL_LOG
    WHERE rows_affected IS NOT NULL
      AND status = 'SUCCESS'
      AND started_at >= DATEADD('day', -14, CURRENT_DATE())
    GROUP BY 1, 2
)
SELECT
    model_name                                               AS "Model",
    run_date                                                 AS "Date",
    ROUND(avg_rows, 0)                                       AS "Avg Rows",
    ROUND(max_rows, 0)                                       AS "Max Rows",
    ROUND(LAG(avg_rows) OVER (
        PARTITION BY model_name ORDER BY run_date
    ), 0)                                                    AS "Prev Day Rows",
    ROUND(
        (avg_rows - LAG(avg_rows) OVER (
            PARTITION BY model_name ORDER BY run_date
        )) * 100.0
        / NULLIF(LAG(avg_rows) OVER (
            PARTITION BY model_name ORDER BY run_date
        ), 0), 1
    )                                                        AS "% Change"
FROM daily_rows
ORDER BY model_name, run_date DESC;

-- ─────────────────────────────────────────────────────────────────
-- E5. Query Compilation vs Execution  [CHART: Stacked bar]
-- Identifies if bottleneck is compilation or execution
-- ─────────────────────────────────────────────────────────────────
SELECT
    query_tag                                                AS "Model Tag",
    ROUND(AVG(compilation_time) / 1000.0, 2)                 AS "Avg Compile (sec)",
    ROUND(AVG(execution_time) / 1000.0, 2)                   AS "Avg Execute (sec)",
    ROUND(AVG(queued_overload_time) / 1000.0, 2)             AS "Avg Queue Wait (sec)",
    ROUND(AVG(total_elapsed_time) / 1000.0, 2)               AS "Avg Total (sec)",
    ROUND(
        AVG(compilation_time) * 100.0
        / NULLIF(AVG(total_elapsed_time), 0), 1
    )                                                        AS "Compile % of Total"
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag LIKE 'dbt_%newsletter%'
  AND query_tag NOT LIKE '%test%'
  AND execution_status = 'SUCCESS'
  AND start_time >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY 1
ORDER BY "Avg Total (sec)" DESC;

-- ─────────────────────────────────────────────────────────────────
-- E6. Bytes Spilled (identifies queries needing optimization)
-- [CHART: Table]
-- ─────────────────────────────────────────────────────────────────
SELECT
    query_tag                                                AS "Model Tag",
    query_id                                                 AS "Query ID",
    ROUND(total_elapsed_time / 1000.0, 2)                    AS "Elapsed (sec)",
    ROUND(bytes_spilled_to_local_storage / POWER(1024, 2), 2)  AS "MB Spilled Local",
    ROUND(bytes_spilled_to_remote_storage / POWER(1024, 2), 2) AS "MB Spilled Remote",
    ROUND(bytes_scanned / POWER(1024, 2), 2)                 AS "MB Scanned",
    rows_produced                                            AS "Rows",
    TO_CHAR(start_time, 'YYYY-MM-DD HH24:MI')               AS "Run At"
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE query_tag LIKE 'dbt_%newsletter%'
  AND (bytes_spilled_to_local_storage > 0
       OR bytes_spilled_to_remote_storage > 0)
  AND start_time >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY bytes_spilled_to_remote_storage DESC
LIMIT 20;


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  SECTION F: ERROR ANALYSIS & RELIABILITY                     ║
-- ║  Failure patterns, error frequency, model health             ║
-- ╚═══════════════════════════════════════════════════════════════╝

-- ─────────────────────────────────────────────────────────────────
-- F1. Error Frequency Heatmap  [CHART: Heatmap, X=date, Y=model]
-- ─────────────────────────────────────────────────────────────────
SELECT
    m.model_name                                             AS "Model",
    DATE(m.started_at)                                       AS "Date",
    SUM(IFF(m.status IN ('FAIL','ERROR'), 1, 0))             AS "Failures",
    SUM(IFF(m.status = 'SUCCESS', 1, 0))                     AS "Successes",
    COUNT(*)                                                 AS "Total Runs"
FROM DBT_MODEL_LOG m
WHERE m.started_at >= DATEADD('day', -14, CURRENT_DATE())
GROUP BY 1, 2
HAVING "Failures" > 0
ORDER BY 2 DESC, 1;

-- ─────────────────────────────────────────────────────────────────
-- F2. Error Messages & Frequency  [CHART: Table]
-- ─────────────────────────────────────────────────────────────────
SELECT
    model_name                                               AS "Model",
    LEFT(COALESCE(error_message, 'No message captured'), 200) AS "Error Message",
    COUNT(*)                                                 AS "Occurrences",
    MIN(started_at)                                          AS "First Seen",
    MAX(started_at)                                          AS "Last Seen",
    DATEDIFF('hour', MAX(started_at), CURRENT_TIMESTAMP())   AS "Hours Since Last"
FROM DBT_MODEL_LOG
WHERE status IN ('FAIL', 'ERROR')
  AND started_at >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY "Occurrences" DESC;

-- ─────────────────────────────────────────────────────────────────
-- F3. Run Success/Failure Timeline  [CHART: Stacked bar, X=date]
-- ─────────────────────────────────────────────────────────────────
SELECT
    DATE(run_started_at)                                     AS "Date",
    SUM(IFF(run_status = 'SUCCESS', 1, 0))                   AS "Successful Runs",
    SUM(IFF(run_status = 'FAILED', 1, 0))                    AS "Failed Runs",
    COUNT(*)                                                 AS "Total Runs",
    ROUND(AVG(run_duration_seconds), 2)                      AS "Avg Duration (sec)"
FROM DBT_RUN_LOG
WHERE run_started_at >= DATEADD('day', -30, CURRENT_DATE())
  AND run_status != 'RUNNING'
GROUP BY 1
ORDER BY 1 DESC;

-- ─────────────────────────────────────────────────────────────────
-- F4. Model Health Matrix  [CHART: Table with conditional coloring]
-- Overall health score per model
-- ─────────────────────────────────────────────────────────────────
SELECT
    model_name                                               AS "Model",
    schema_name                                              AS "Schema",
    COUNT(*)                                                 AS "Total Runs",
    SUM(IFF(status = 'SUCCESS', 1, 0))                       AS "Successes",
    SUM(IFF(status IN ('FAIL','ERROR'), 1, 0))               AS "Failures",
    ROUND(
        SUM(IFF(status = 'SUCCESS', 1, 0)) * 100.0
        / NULLIF(COUNT(*), 0), 1
    )                                                        AS "Success Rate %",
    MAX(started_at)                                          AS "Last Run",
    DATEDIFF('hour', MAX(started_at), CURRENT_TIMESTAMP())   AS "Hours Since Last Run",
    CASE
        WHEN SUM(IFF(status IN ('FAIL','ERROR'), 1, 0)) = 0
            THEN 'HEALTHY'
        WHEN SUM(IFF(status IN ('FAIL','ERROR'), 1, 0))
            <= COUNT(*) * 0.1
            THEN 'WARNING'
        ELSE 'CRITICAL'
    END                                                      AS "Health"
FROM DBT_MODEL_LOG
WHERE started_at >= DATEADD('day', -14, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY "Success Rate %" ASC;


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  SECTION G: DATA FRESHNESS & QUALITY                         ║
-- ║  Current state of mart tables, data quality signals          ║
-- ╚═══════════════════════════════════════════════════════════════╝

-- ─────────────────────────────────────────────────────────────────
-- G1. Data Freshness  [CHART: Scorecard / Table]
-- ─────────────────────────────────────────────────────────────────
SELECT
    'wrk_newsletter'                                         AS "Model",
    COUNT(*)                                                 AS "Rows",
    MAX(dbt_loaded_at)                                       AS "Last Loaded",
    DATEDIFF('minute', MAX(dbt_loaded_at), CURRENT_TIMESTAMP()) AS "Minutes Stale",
    IFF(DATEDIFF('minute', MAX(dbt_loaded_at), CURRENT_TIMESTAMP()) > 1440,
        'STALE', 'FRESH')                                    AS "Status"
FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER
UNION ALL
SELECT
    'wrk_newsletter_interaction',
    COUNT(*),
    MAX(dbt_loaded_at),
    DATEDIFF('minute', MAX(dbt_loaded_at), CURRENT_TIMESTAMP()),
    IFF(DATEDIFF('minute', MAX(dbt_loaded_at), CURRENT_TIMESTAMP()) > 1440,
        'STALE', 'FRESH')
FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER_INTERACTION
UNION ALL
SELECT
    'wrk_newsletter_category',
    COUNT(*),
    MAX(dbt_loaded_at),
    DATEDIFF('minute', MAX(dbt_loaded_at), CURRENT_TIMESTAMP()),
    IFF(DATEDIFF('minute', MAX(dbt_loaded_at), CURRENT_TIMESTAMP()) > 1440,
        'STALE', 'FRESH')
FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER_CATEGORY
ORDER BY "Minutes Stale" DESC;

-- ─────────────────────────────────────────────────────────────────
-- G2. Data Quality Scorecard  [CHART: Table with % indicators]
-- ─────────────────────────────────────────────────────────────────
SELECT
    'wrk_newsletter'                                         AS "Model",
    COUNT(*)                                                 AS "Total Rows",
    ROUND(SUM(IFF(tenant_code = 'N/A', 1, 0)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                            AS "% Unknown Tenant",
    ROUND(SUM(IFF(status_code = 'NLS000', 1, 0)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                            AS "% Unmapped Status",
    ROUND(SUM(IFF(data_source_code IS NULL, 1, 0)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                            AS "% Null Data Source",
    ROUND(SUM(IFF(hash_value IS NULL, 1, 0)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                            AS "% Null Hash",
    ROUND(SUM(IFF(is_deleted, 1, 0)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                            AS "% Soft Deleted",
    ROUND(SUM(IFF(active_flag, 1, 0)) * 100.0
        / NULLIF(COUNT(*), 0), 2)                            AS "% Active"
FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER;

-- ─────────────────────────────────────────────────────────────────
-- G3. Snapshot (SCD-2) Tracking  [CHART: Table]
-- Shows versions per entity in the snapshot table
-- ─────────────────────────────────────────────────────────────────
SELECT
    tenant_code                                              AS "Tenant",
    code                                                     AS "Newsletter Code",
    name                                                     AS "Name",
    COUNT(*)                                                 AS "Total Versions",
    SUM(IFF(dbt_valid_to IS NULL, 1, 0))                     AS "Active Versions",
    MIN(dbt_valid_from)                                      AS "First Version",
    MAX(dbt_valid_from)                                      AS "Latest Version"
FROM COMMON_TENANT_DEV.DBT_UDL.SNAP_NEWSLETTER
GROUP BY 1, 2, 3
ORDER BY "Total Versions" DESC
LIMIT 50;

-- ─────────────────────────────────────────────────────────────────
-- G4. Record Distribution by Tenant  [CHART: Bar chart]
-- ─────────────────────────────────────────────────────────────────
SELECT
    tenant_code                                              AS "Tenant Code",
    data_source_code                                         AS "Source Type",
    COUNT(*)                                                 AS "Newsletters",
    SUM(IFF(is_deleted, 1, 0))                               AS "Deleted",
    SUM(IFF(status_code != 'NLS000', 1, 0))                  AS "Mapped Status"
FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER
GROUP BY 1, 2
ORDER BY "Newsletters" DESC;


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  SECTION H: OPERATIONAL ALERTS                               ║
-- ║  Queries designed to power Snowflake ALERT objects            ║
-- ╚═══════════════════════════════════════════════════════════════╝

-- ─────────────────────────────────────────────────────────────────
-- H1. Active Alerts Summary  [CHART: Table]
-- ─────────────────────────────────────────────────────────────────
SELECT 'STALE_DATA' AS "Alert Type", model_name AS "Subject",
    'Data is ' || minutes_since_last_load || ' minutes old' AS "Detail"
FROM V_DATA_FRESHNESS
WHERE minutes_since_last_load > 1440

UNION ALL

SELECT 'FAILED_RUN', run_id,
    models_failed || ' model(s) failed at ' || TO_CHAR(run_started_at, 'HH24:MI')
FROM DBT_RUN_LOG
WHERE run_status = 'FAILED'
  AND run_started_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP())

UNION ALL

SELECT 'ROW_ANOMALY', model_name,
    'Row count dropped ' || ABS(pct_change) || '% on ' || run_date
FROM V_ROW_COUNT_TREND
WHERE pct_change < -50
  AND run_date >= DATEADD('day', -3, CURRENT_DATE())

UNION ALL

SELECT 'CONSECUTIVE_FAIL', model_name,
    consecutive_failures || ' consecutive failures since ' || failure_streak_start
FROM V_CONSECUTIVE_FAILURES

ORDER BY "Alert Type";


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║  SECTION I: DATA RECONCILIATION                              ║
-- ║  Source-to-target completeness, cross-entity integrity,      ║
-- ║  duplicate detection, hash-based gap analysis                ║
-- ╚═══════════════════════════════════════════════════════════════╝

-- ─────────────────────────────────────────────────────────────────
-- I1. Source → Staging → Mart Row Counts  [CHART: Table]
-- Compare record counts across all three layers per entity
-- ─────────────────────────────────────────────────────────────────
SELECT
    'Newsletter'                                             AS "Entity",
    (SELECT COUNT(*) FROM COMMON_TENANT_DEV.SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER)
                                                             AS "Source Rows",
    (SELECT COUNT(*) FROM COMMON_TENANT_DEV.UDL.STG_NEWSLETTER)
                                                             AS "Staging Rows",
    (SELECT COUNT(*) FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER)
                                                             AS "Mart Rows",
    (SELECT COUNT(DISTINCT code) FROM COMMON_TENANT_DEV.SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER
     WHERE domain_payload:id::STRING IS NOT NULL)
                                                             AS "Source Distinct Codes",
    (SELECT COUNT(*) FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER)
                                                             AS "Mart Distinct Codes",
    IFF("Source Distinct Codes" = "Mart Distinct Codes",
        'MATCH', 'MISMATCH')                                 AS "Status"

UNION ALL

SELECT
    'Newsletter Interaction',
    (SELECT COUNT(*) FROM COMMON_TENANT_DEV.SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_INTERACTION),
    (SELECT COUNT(*) FROM COMMON_TENANT_DEV.UDL.STG_NEWSLETTER_INTERACTION),
    (SELECT COUNT(*) FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER_INTERACTION),
    (SELECT COUNT(DISTINCT domain_payload:id::STRING)
     FROM COMMON_TENANT_DEV.SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_INTERACTION
     WHERE domain_payload:id::STRING IS NOT NULL),
    (SELECT COUNT(*) FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER_INTERACTION),
    IFF(
        (SELECT COUNT(DISTINCT domain_payload:id::STRING)
         FROM COMMON_TENANT_DEV.SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_INTERACTION
         WHERE domain_payload:id::STRING IS NOT NULL)
        = (SELECT COUNT(*) FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER_INTERACTION),
        'MATCH', 'MISMATCH')

UNION ALL

SELECT
    'Newsletter Category',
    (SELECT COUNT(*) FROM COMMON_TENANT_DEV.SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_CATEGORY),
    (SELECT COUNT(*) FROM COMMON_TENANT_DEV.UDL.STG_NEWSLETTER_CATEGORY),
    (SELECT COUNT(*) FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER_CATEGORY),
    (SELECT COUNT(DISTINCT domain_payload:id::STRING)
     FROM COMMON_TENANT_DEV.SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_CATEGORY
     WHERE domain_payload:id::STRING IS NOT NULL),
    (SELECT COUNT(*) FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER_CATEGORY),
    IFF(
        (SELECT COUNT(DISTINCT domain_payload:id::STRING)
         FROM COMMON_TENANT_DEV.SHARED_SERVICES_STAGING.VW_ENL_NEWSLETTER_CATEGORY
         WHERE domain_payload:id::STRING IS NOT NULL)
        = (SELECT COUNT(*) FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER_CATEGORY),
        'MATCH', 'MISMATCH');


-- ─────────────────────────────────────────────────────────────────
-- I2. Cross-Entity Referential Integrity  [CHART: Table]
-- Validate that interactions reference valid newsletters,
-- and newsletters reference valid categories
-- ─────────────────────────────────────────────────────────────────
SELECT
    'Interactions → Newsletters'                             AS "Relationship",
    COUNT(DISTINCT i.newsletter_code)                        AS "Distinct FK Values",
    SUM(IFF(n.code IS NOT NULL, 1, 0))                       AS "Matched",
    SUM(IFF(n.code IS NULL, 1, 0))                           AS "Orphaned",
    ROUND(
        SUM(IFF(n.code IS NOT NULL, 1, 0)) * 100.0
        / NULLIF(COUNT(*), 0), 1
    )                                                        AS "Integrity %"
FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER_INTERACTION i
LEFT JOIN COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER n
    ON i.tenant_code = n.tenant_code
   AND i.newsletter_code = n.code

UNION ALL

SELECT
    'Newsletters → Categories',
    COUNT(DISTINCT nl.category_code),
    SUM(IFF(nl.category_code = 'N/A' OR c.code IS NOT NULL, 1, 0)),
    SUM(IFF(nl.category_code != 'N/A' AND c.code IS NULL, 1, 0)),
    ROUND(
        SUM(IFF(nl.category_code = 'N/A' OR c.code IS NOT NULL, 1, 0)) * 100.0
        / NULLIF(COUNT(*), 0), 1
    )
FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER nl
LEFT JOIN COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER_CATEGORY c
    ON nl.tenant_code = c.tenant_code
   AND nl.category_code = c.code
WHERE nl.category_code != 'N/A';


-- ─────────────────────────────────────────────────────────────────
-- I3. Orphaned Interaction Details  [CHART: Table]
-- Lists interactions that reference newsletters not in mart
-- ─────────────────────────────────────────────────────────────────
SELECT
    i.tenant_code                                            AS "Tenant",
    i.code                                                   AS "Interaction Code",
    i.newsletter_code                                        AS "Newsletter Code (FK)",
    i.interaction_type_code                                  AS "Type",
    i.interaction_datetime                                   AS "Interaction At",
    'Newsletter not found in wrk_newsletter'                 AS "Issue"
FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER_INTERACTION i
LEFT JOIN COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER n
    ON i.tenant_code = n.tenant_code
   AND i.newsletter_code = n.code
WHERE n.code IS NULL
ORDER BY i.interaction_datetime DESC
LIMIT 50;


-- ─────────────────────────────────────────────────────────────────
-- I4. Duplicate Detection in Marts  [CHART: Table]
-- Checks for duplicate (tenant_code, code) in each mart
-- Should return 0 rows if data is clean
-- ─────────────────────────────────────────────────────────────────
SELECT
    'wrk_newsletter'                                         AS "Model",
    tenant_code                                              AS "Tenant",
    code                                                     AS "Code",
    COUNT(*)                                                 AS "Duplicate Count"
FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1

UNION ALL

SELECT
    'wrk_newsletter_interaction',
    tenant_code,
    code,
    COUNT(*)
FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER_INTERACTION
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1

UNION ALL

SELECT
    'wrk_newsletter_category',
    tenant_code,
    code,
    COUNT(*)
FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER_CATEGORY
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1

ORDER BY "Duplicate Count" DESC;


-- ─────────────────────────────────────────────────────────────────
-- I5. Source Records Missing from Mart  [CHART: Table]
-- Hash-based gap analysis: source codes not yet in mart
-- ─────────────────────────────────────────────────────────────────
SELECT
    'Newsletter'                                             AS "Entity",
    src.tenant_code                                          AS "Tenant",
    src.code                                                 AS "Code",
    src.kafka_timestamp                                      AS "Source Timestamp",
    'Not in wrk_newsletter'                                  AS "Gap"
FROM COMMON_TENANT_DEV.UDL.STG_NEWSLETTER src
LEFT JOIN COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER tgt
    ON src.tenant_code = tgt.tenant_code
   AND src.code = tgt.code
WHERE tgt.code IS NULL
  AND src.code IS NOT NULL
  AND src.tenant_code IS NOT NULL

UNION ALL

SELECT
    'Interaction',
    src.tenant_code,
    src.code,
    src.kafka_timestamp,
    'Not in wrk_newsletter_interaction'
FROM COMMON_TENANT_DEV.UDL.STG_NEWSLETTER_INTERACTION src
LEFT JOIN COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER_INTERACTION tgt
    ON src.tenant_code = tgt.tenant_code
   AND src.code = tgt.code
WHERE tgt.code IS NULL
  AND src.code IS NOT NULL
  AND src.tenant_code IS NOT NULL

UNION ALL

SELECT
    'Category',
    src.tenant_code,
    src.code,
    src.kafka_timestamp,
    'Not in wrk_newsletter_category'
FROM COMMON_TENANT_DEV.UDL.STG_NEWSLETTER_CATEGORY src
LEFT JOIN COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER_CATEGORY tgt
    ON src.tenant_code = tgt.tenant_code
   AND src.code = tgt.code
WHERE tgt.code IS NULL
  AND src.code IS NOT NULL
  AND src.tenant_code IS NOT NULL

ORDER BY "Entity", "Source Timestamp" DESC;


-- ─────────────────────────────────────────────────────────────────
-- I6. Snapshot vs Mart Consistency  [CHART: Table]
-- Verifies every active mart record has a current snapshot version
-- ─────────────────────────────────────────────────────────────────
SELECT
    'Active in mart, missing in snapshot'                     AS "Check",
    COUNT(*)                                                 AS "Records"
FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER m
LEFT JOIN COMMON_TENANT_DEV.DBT_UDL.SNAP_NEWSLETTER s
    ON m.tenant_code = s.tenant_code
   AND m.code = s.code
   AND s.dbt_valid_to IS NULL
WHERE s.code IS NULL

UNION ALL

SELECT
    'Snapshot hash != mart hash (stale snapshot)',
    COUNT(*)
FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER m
INNER JOIN COMMON_TENANT_DEV.DBT_UDL.SNAP_NEWSLETTER s
    ON m.tenant_code = s.tenant_code
   AND m.code = s.code
   AND s.dbt_valid_to IS NULL
WHERE m.hash_value != s.hash_value

UNION ALL

SELECT
    'Snapshot versions per newsletter (avg)',
    ROUND(AVG(version_count), 1)
FROM (
    SELECT tenant_code, code, COUNT(*) AS version_count
    FROM COMMON_TENANT_DEV.DBT_UDL.SNAP_NEWSLETTER
    GROUP BY 1, 2
);


-- ─────────────────────────────────────────────────────────────────
-- I7. Reconciliation Summary Scorecard  [CHART: Scorecard / KPI]
-- Single-row summary of all reconciliation checks
-- ─────────────────────────────────────────────────────────────────
SELECT
    (SELECT COUNT(*) FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER)
        + (SELECT COUNT(*) FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER_INTERACTION)
        + (SELECT COUNT(*) FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER_CATEGORY)
                                                             AS "Total Mart Records",

    (SELECT COUNT(*) FROM COMMON_TENANT_DEV.UDL.STG_NEWSLETTER src
     LEFT JOIN COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER tgt
         ON src.tenant_code = tgt.tenant_code AND src.code = tgt.code
     WHERE tgt.code IS NULL AND src.code IS NOT NULL AND src.tenant_code IS NOT NULL)
        + (SELECT COUNT(*) FROM COMMON_TENANT_DEV.UDL.STG_NEWSLETTER_INTERACTION src
           LEFT JOIN COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER_INTERACTION tgt
               ON src.tenant_code = tgt.tenant_code AND src.code = tgt.code
           WHERE tgt.code IS NULL AND src.code IS NOT NULL AND src.tenant_code IS NOT NULL)
        + (SELECT COUNT(*) FROM COMMON_TENANT_DEV.UDL.STG_NEWSLETTER_CATEGORY src
           LEFT JOIN COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER_CATEGORY tgt
               ON src.tenant_code = tgt.tenant_code AND src.code = tgt.code
           WHERE tgt.code IS NULL AND src.code IS NOT NULL AND src.tenant_code IS NOT NULL)
                                                             AS "Missing from Mart",

    (SELECT COUNT(*) FROM (
        SELECT tenant_code, code FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER
        GROUP BY 1, 2 HAVING COUNT(*) > 1
        UNION ALL
        SELECT tenant_code, code FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER_INTERACTION
        GROUP BY 1, 2 HAVING COUNT(*) > 1
        UNION ALL
        SELECT tenant_code, code FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER_CATEGORY
        GROUP BY 1, 2 HAVING COUNT(*) > 1
    ))                                                       AS "Duplicate Keys",

    (SELECT COUNT(*) FROM COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER_INTERACTION i
     LEFT JOIN COMMON_TENANT_DEV.DBT_UDL.WRK_NEWSLETTER n
         ON i.tenant_code = n.tenant_code AND i.newsletter_code = n.code
     WHERE n.code IS NULL)
                                                             AS "Orphaned Interactions",

    IFF("Missing from Mart" = 0
        AND "Duplicate Keys" = 0
        AND "Orphaned Interactions" = 0,
        'ALL CHECKS PASSED', 'ISSUES FOUND')
                                                             AS "Overall Status";
