{#
═══════════════════════════════════════════════════════════════════════════════
RUN LOGGING MACROS
═══════════════════════════════════════════════════════════════════════════════

Called by on-run-start / on-run-end hooks in dbt_project.yml.
Writes to COMMON_TENANT_DEV.DBT_EXECUTION_RUN_STATUS.DBT_RUN_LOG.

Prerequisites:
  Run setup/audit_setup.sql to create tracking tables.

═══════════════════════════════════════════════════════════════════════════════
#}


{% macro log_run_start() %}
    {% if var('enable_audit_logging', true) %}
        {% set sql %}
            INSERT INTO {{ target.database }}.{{ var('audit_schema', 'DBT_EXECUTION_RUN_STATUS') }}.DBT_RUN_LOG (
                run_id,
                project_name,
                environment,
                run_started_at,
                run_status,
                warehouse_name,
                user_name,
                role_name
            )
            SELECT
                '{{ invocation_id }}',
                '{{ project_name }}',
                '{{ target.name }}',
                '{{ run_started_at }}'::TIMESTAMP_NTZ,
                'RUNNING',
                CURRENT_WAREHOUSE(),
                CURRENT_USER(),
                CURRENT_ROLE()
            WHERE NOT EXISTS (
                SELECT 1 FROM {{ target.database }}.{{ var('audit_schema', 'DBT_EXECUTION_RUN_STATUS') }}.DBT_RUN_LOG
                WHERE run_id = '{{ invocation_id }}'
            );
        {% endset %}

        {% do run_query(sql) %}
        {% do log("Run logging started: " ~ invocation_id, info=true) %}
    {% endif %}
{% endmacro %}


{% macro log_run_end() %}
    {% if var('enable_audit_logging', true) %}
        {% set audit_db_schema = target.database ~ '.' ~ var('audit_schema', 'SIMPPLR_DBT_AUDIT') %}
        {% set sql %}
            UPDATE {{ audit_db_schema }}.DBT_RUN_LOG
            SET
                run_ended_at = CURRENT_TIMESTAMP(),
                run_duration_seconds = DATEDIFF('second', run_started_at, CURRENT_TIMESTAMP()),
                run_status = CASE
                    WHEN (SELECT COUNT(*) FROM {{ audit_db_schema }}.DBT_MODEL_LOG
                          WHERE run_id = '{{ invocation_id }}' AND status = 'FAIL') > 0
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
        {% do log("Run logging completed: " ~ invocation_id, info=true) %}
    {% endif %}
{% endmacro %}
