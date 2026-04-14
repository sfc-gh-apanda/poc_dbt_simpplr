{#
═══════════════════════════════════════════════════════════════════════════════
PUBLISH TO TARGET MACRO
═══════════════════════════════════════════════════════════════════════════════

Called as a post-hook on the pipeline_complete sentinel model.
Invokes PRC_DBT_PUBLISH_TO_TARGET which performs dynamic, entity-driven publish:
  NEWSLETTER: wrk delta → NEWSLETTER_HIST (SCD-2) → TRUNCATE+INSERT DBT_UDL.NEWSLETTER
  INTERACTION / CATEGORY: delta MERGE into DBT_UDL (preserves Time Travel)
  Full load mode: TRUNCATE+INSERT (skips MERGE/deactivate pattern)
All within a single transaction for atomicity.

Passes invocation_id, batch_run_id, and full_load_entity for end-to-end
traceability and full load control.

Variables:
  enable_publish              : true/false (default true)
  batch_run_id                : Airflow batch run ID (default 0)
  is_full_load                : Global full load flag (default false)
  entity_specific_full_load   : Comma-separated entity names (default 'none')

Prerequisites:
  Run setup/publish_archive_setup.sql to create the stored procedure.

═══════════════════════════════════════════════════════════════════════════════
#}

{% macro publish_to_target() %}
    {% if var('enable_publish', true) and execute %}

        {# Resolve full load entity flag for the procedure #}
        {% if var('is_full_load', false) %}
            {% set full_load_entity = 'ALL' %}
        {% else %}
            {% set full_load_entity = var('entity_specific_full_load', 'NONE') | upper %}
        {% endif %}

        {% set sql %}
            CALL DBT_UDL_BATCH_PROCESS.PRC_DBT_PUBLISH_TO_TARGET(
                '{{ invocation_id }}',
                {{ var('batch_run_id', 0) }},
                '{{ full_load_entity }}'
            );
        {% endset %}

        {% set result = run_query(sql) %}

        {% if result and result.columns | length > 0 %}
            {% set response = result.columns[0].values()[0] %}
            {{ log("Publish to target result: " ~ response, info=true) }}
        {% endif %}

    {% endif %}
{% endmacro %}
