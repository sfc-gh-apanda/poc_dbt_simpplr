{#
═══════════════════════════════════════════════════════════════════════════════
RUN LOGGING MACROS
═══════════════════════════════════════════════════════════════════════════════

Called by on-run-start / on-run-end hooks in dbt_project.yml.
Writes to COMMON_TENANT_DEV.DBT_EXECUTION_RUN_STATS.DBT_RUN_LOG.

Prerequisites:
  Run setup/audit_setup.sql to create tracking tables.

═══════════════════════════════════════════════════════════════════════════════
#}


{% macro log_run_start() %}
    {% if var('enable_audit_logging', true) and execute %}
        {% set audit_db_schema = target.database ~ '.' ~ var('audit_schema', 'DBT_EXECUTION_RUN_STATS') %}
        {% set sql %}
            MERGE INTO {{ audit_db_schema }}.DBT_RUN_LOG tgt
            USING (
                SELECT
                    '{{ invocation_id }}'       AS run_id,
                    {{ var('batch_run_id', 0) }} AS batch_run_id,
                    '{{ project_name }}'         AS project_name,
                    '{{ target.name }}'          AS environment,
                    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS run_started_at,
                    'RUNNING'                    AS run_status,
                    CURRENT_WAREHOUSE()          AS warehouse_name,
                    CURRENT_USER()               AS user_name,
                    CURRENT_ROLE()               AS role_name
            ) src ON tgt.run_id = src.run_id
            WHEN NOT MATCHED THEN INSERT (
                run_id, batch_run_id, project_name, environment,
                run_started_at, run_status, warehouse_name, user_name, role_name
            ) VALUES (
                src.run_id, src.batch_run_id, src.project_name, src.environment,
                src.run_started_at, src.run_status, src.warehouse_name, src.user_name, src.role_name
            );
        {% endset %}

        {% do run_query(sql) %}
        {% do run_query("COMMIT") %}
        {% do log("Run logging started: " ~ invocation_id, info=true) %}
    {% endif %}
{% endmacro %}


{% macro log_run_end() %}
    {% if var('enable_audit_logging', true) and execute %}
        {% set audit_db_schema = target.database ~ '.' ~ var('audit_schema', 'DBT_EXECUTION_RUN_STATS') %}
        {% set sql %}
            UPDATE {{ audit_db_schema }}.DBT_RUN_LOG
            SET
                batch_run_id = {{ var('batch_run_id', 0) }},
                run_ended_at = CURRENT_TIMESTAMP(),
                run_duration_seconds = DATEDIFF('second', run_started_at, CURRENT_TIMESTAMP()),
                run_status = CASE
                    WHEN (SELECT COUNT(*) FROM {{ audit_db_schema }}.DBT_MODEL_LOG
                          WHERE run_id = '{{ invocation_id }}' AND status IN ('FAIL', 'ERROR')) > 0
                    THEN 'FAILED'
                    ELSE 'SUCCESS'
                END,
                models_run = (SELECT COUNT(*) FROM {{ audit_db_schema }}.DBT_MODEL_LOG
                              WHERE run_id = '{{ invocation_id }}'),
                models_success = (SELECT COUNT(*) FROM {{ audit_db_schema }}.DBT_MODEL_LOG
                                  WHERE run_id = '{{ invocation_id }}' AND status = 'SUCCESS'),
                models_failed = (SELECT COUNT(*) FROM {{ audit_db_schema }}.DBT_MODEL_LOG
                                 WHERE run_id = '{{ invocation_id }}' AND status IN ('FAIL', 'ERROR'))
            WHERE run_id = '{{ invocation_id }}';
        {% endset %}

        {% do run_query(sql) %}
        {% do run_query("COMMIT") %}
        {% do log("Run logging completed: " ~ invocation_id, info=true) %}
    {% endif %}
{% endmacro %}
