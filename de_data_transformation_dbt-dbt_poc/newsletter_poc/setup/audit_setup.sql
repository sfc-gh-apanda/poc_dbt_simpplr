-- =============================================================
-- SIMPPLR DBT PoC — Audit & Monitoring Tables Setup
-- Run ONCE after account_setup.sql
-- =============================================================

USE ROLE R_DEPLOYMENT_ADMIN_DEV;
USE DATABASE COMMON_TENANT_DEV;

-- 1. Audit schema
CREATE SCHEMA IF NOT EXISTS DBT_EXECUTION_RUN_STATS
    COMMENT = 'dbt processing tracking, audit, and observability';

USE SCHEMA DBT_EXECUTION_RUN_STATS;

-- =============================================================
-- 2. DBT_RUN_LOG — one row per dbt invocation
-- =============================================================

CREATE TABLE IF NOT EXISTS DBT_RUN_LOG (
    run_id                  VARCHAR(50)     PRIMARY KEY     COMMENT 'dbt invocation_id',
    project_name            VARCHAR(100)                    COMMENT 'dbt project name',
    environment             VARCHAR(20)                     COMMENT 'Target environment (dev/prod)',
    run_started_at          TIMESTAMP_NTZ                   COMMENT 'Run start timestamp',
    run_ended_at            TIMESTAMP_NTZ                   COMMENT 'Run end timestamp',
    run_duration_seconds    NUMBER(10,2)                    COMMENT 'Total duration in seconds',
    run_status              VARCHAR(20)                     COMMENT 'RUNNING, SUCCESS, FAILED',
    models_run              INTEGER                         COMMENT 'Total models executed',
    models_success          INTEGER                         COMMENT 'Models succeeded',
    models_failed           INTEGER                         COMMENT 'Models failed',
    warehouse_name          VARCHAR(100)                    COMMENT 'Snowflake warehouse used',
    user_name               VARCHAR(100)                    COMMENT 'Snowflake user',
    role_name               VARCHAR(100)                    COMMENT 'Snowflake role',
    created_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================
-- 3. DBT_MODEL_LOG — one row per model per run
-- =============================================================

CREATE TABLE IF NOT EXISTS DBT_MODEL_LOG (
    log_id                  VARCHAR(100)    PRIMARY KEY     COMMENT 'Unique log entry (run_id + model)',
    run_id                  VARCHAR(50)                     COMMENT 'FK to DBT_RUN_LOG',
    project_name            VARCHAR(100)                    COMMENT 'dbt project name',
    model_name              VARCHAR(200)                    COMMENT 'Model name',
    model_alias             VARCHAR(200)                    COMMENT 'Model alias if different',
    schema_name             VARCHAR(100)                    COMMENT 'Target schema',
    database_name           VARCHAR(100)                    COMMENT 'Target database',
    materialization         VARCHAR(50)                     COMMENT 'table / view / incremental',
    batch_id                VARCHAR(50)                     COMMENT 'Unique batch ID per model per run',
    status                  VARCHAR(20)                     COMMENT 'SUCCESS / FAIL / ERROR / SKIPPED',
    error_message           VARCHAR(4000)                   COMMENT 'Error details if failed',
    started_at              TIMESTAMP_NTZ                   COMMENT 'Model execution start',
    ended_at                TIMESTAMP_NTZ                   COMMENT 'Model execution end',
    rows_affected           INTEGER                         COMMENT 'Rows produced / affected',
    is_incremental          BOOLEAN                         COMMENT 'Incremental model flag',
    incremental_strategy    VARCHAR(50)                     COMMENT 'merge / append / delete+insert',
    created_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================
-- 4. Summary views for monitoring
-- =============================================================

CREATE OR REPLACE VIEW V_DAILY_RUN_SUMMARY AS
SELECT
    DATE(run_started_at)                                             AS run_date,
    COUNT(*)                                                         AS total_runs,
    SUM(CASE WHEN run_status = 'SUCCESS' THEN 1 ELSE 0 END)         AS successful_runs,
    SUM(CASE WHEN run_status = 'FAILED'  THEN 1 ELSE 0 END)         AS failed_runs,
    SUM(models_run)                                                  AS total_models_run,
    SUM(models_success)                                              AS total_models_success,
    SUM(models_failed)                                               AS total_models_failed,
    ROUND(AVG(run_duration_seconds), 2)                              AS avg_run_duration_sec,
    ROUND(MAX(run_duration_seconds), 2)                              AS max_run_duration_sec
FROM DBT_RUN_LOG
WHERE run_started_at >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1
ORDER BY 1 DESC;

CREATE OR REPLACE VIEW V_MODEL_EXECUTION_HISTORY AS
SELECT
    m.model_name,
    m.schema_name,
    m.materialization,
    COUNT(*)                                                         AS total_runs,
    SUM(CASE WHEN m.status = 'SUCCESS' THEN 1 ELSE 0 END)           AS successful_runs,
    SUM(CASE WHEN m.status = 'FAIL'    THEN 1 ELSE 0 END)           AS failed_runs,
    AVG(m.rows_affected)                                             AS avg_rows_affected,
    MAX(m.started_at)                                                AS last_run_at,
    r.environment
FROM DBT_MODEL_LOG m
LEFT JOIN DBT_RUN_LOG r ON m.run_id = r.run_id
WHERE m.started_at >= DATEADD('day', -7, CURRENT_DATE())
GROUP BY m.model_name, m.schema_name, m.materialization, r.environment
ORDER BY total_runs DESC;

CREATE OR REPLACE VIEW V_RECENT_FAILURES AS
SELECT
    m.run_id,
    m.model_name,
    m.schema_name,
    m.status,
    m.error_message,
    m.started_at,
    r.environment,
    r.user_name
FROM DBT_MODEL_LOG m
LEFT JOIN DBT_RUN_LOG r ON m.run_id = r.run_id
WHERE m.status IN ('FAIL', 'ERROR')
  AND m.started_at >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY m.started_at DESC;

CREATE OR REPLACE VIEW V_INCREMENTAL_MODEL_STATS AS
SELECT
    m.model_name,
    m.incremental_strategy,
    COUNT(*)                                                         AS total_runs,
    AVG(m.rows_affected)                                             AS avg_rows_per_run,
    MAX(m.rows_affected)                                             AS max_rows_per_run,
    MIN(m.started_at)                                                AS first_run_at,
    MAX(m.started_at)                                                AS last_run_at
FROM DBT_MODEL_LOG m
WHERE m.is_incremental = TRUE
GROUP BY m.model_name, m.incremental_strategy
ORDER BY last_run_at DESC;




