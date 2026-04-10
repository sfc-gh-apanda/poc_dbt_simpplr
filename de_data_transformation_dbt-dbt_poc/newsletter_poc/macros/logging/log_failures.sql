{#
═══════════════════════════════════════════════════════════════════════════════
FAILURE LOGGING MACRO
═══════════════════════════════════════════════════════════════════════════════

Called in on-run-end BEFORE log_run_end().

The standard post-hook (log_model_execution) only fires on SUCCESS because
dbt skips post-hooks when a model fails. This macro fills the gap by
iterating over dbt's `results` context — available only in on-run-end — to
capture FAIL / ERROR / SKIPPED entries into DBT_MODEL_LOG.

This gives PRC_DBT_SMART_RETRY reliable data to determine what needs re-run.

═══════════════════════════════════════════════════════════════════════════════
#}


{% macro log_failed_models() %}
    {% if var('enable_audit_logging', true) and execute %}
        {% set audit_db_schema = target.database ~ '.' ~ var('audit_schema', 'DBT_EXECUTION_RUN_STATS') %}

        {% for result in results %}
            {% if result.status in ['error', 'fail', 'skipped'] %}

                {% set log_id = invocation_id ~ '_' ~ result.node.name %}

                {% set safe_message = '' %}
                {% if result.message %}
                    {% set safe_message = result.message | replace("'", "''") | truncate(3900, True) %}
                {% endif %}

                {% set mapped_status = 'FAIL' %}
                {% if result.status == 'skipped' %}
                    {% set mapped_status = 'SKIPPED' %}
                {% endif %}

                {% set sql %}
                    INSERT INTO {{ audit_db_schema }}.DBT_MODEL_LOG (
                        log_id, run_id, project_name, model_name,
                        schema_name, database_name, materialization,
                        batch_id, status, error_message,
                        started_at, ended_at,
                        rows_affected, is_incremental
                    )
                    SELECT
                        '{{ log_id }}',
                        '{{ invocation_id }}',
                        '{{ project_name }}',
                        '{{ result.node.name }}',
                        '{{ result.node.schema }}',
                        '{{ result.node.database }}',
                        '{{ result.node.config.materialized }}',
                        '{{ modules.datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S") }}_{{ result.node.name }}',
                        '{{ mapped_status }}',
                        '{{ safe_message }}',
                        CURRENT_TIMESTAMP(),
                        CURRENT_TIMESTAMP(),
                        0,
                        {{ 'TRUE' if result.node.config.materialized == 'incremental' else 'FALSE' }}
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {{ audit_db_schema }}.DBT_MODEL_LOG
                        WHERE log_id = '{{ log_id }}'
                    )
                {% endset %}

                {% do run_query(sql) %}
                {% do log("Logged " ~ mapped_status ~ " for model: " ~ result.node.name ~ " — " ~ result.message, info=true) %}

            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}
