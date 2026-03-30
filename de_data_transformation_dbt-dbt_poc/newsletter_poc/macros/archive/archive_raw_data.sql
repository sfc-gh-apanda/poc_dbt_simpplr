{#
═══════════════════════════════════════════════════════════════════════════════
ARCHIVE RAW DATA MACRO
═══════════════════════════════════════════════════════════════════════════════

Called as a post-hook on the pipeline_complete sentinel model, AFTER
publish_to_target(). Archives processed raw records from ENL_* tables into
ENL_*_ARCHIVE tables, then purges the originals.

Guarded by var('enable_archive') — defaults to false, matching the existing
Airflow DAG's archive_raw_data_flag behavior.

Uses var('data_process_end_time') as the archival boundary — this must match
the same value used in the staging models to ensure only processed records
are archived.

Prerequisites:
  Run setup/publish_archive_setup.sql to create the stored procedure.

═══════════════════════════════════════════════════════════════════════════════
#}

{% macro archive_raw_data() %}
    {% if var('enable_archive', false) and execute %}

        {% set end_time = var('data_process_end_time', '9999-12-31 23:59:59') %}

        {% set sql %}
            CALL UDL_BATCH_PROCESS.PRC_DBT_ARCHIVE_RAW_DATA(
                '{{ invocation_id }}',
                '{{ end_time }}'
            );
        {% endset %}

        {% set result = run_query(sql) %}

        {% if result and result.columns | length > 0 %}
            {% set response = result.columns[0].values()[0] %}
            {{ log("Archive raw data result: " ~ response, info=true) }}
        {% endif %}

    {% else %}
        {{ log("Archive skipped (enable_archive=" ~ var('enable_archive', false) ~ ")", info=true) }}
    {% endif %}
{% endmacro %}
