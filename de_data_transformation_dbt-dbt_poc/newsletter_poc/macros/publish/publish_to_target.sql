{#
═══════════════════════════════════════════════════════════════════════════════
PUBLISH TO TARGET MACRO
═══════════════════════════════════════════════════════════════════════════════

Called as a post-hook on the pipeline_complete sentinel model.
Invokes PRC_DBT_PUBLISH_TO_TARGET which atomically clones all 3 mart tables
from DBT_UDL → UDL and appends to NEWSLETTER_HIST, in a single transaction.

Prerequisites:
  Run setup/publish_archive_setup.sql to create the stored procedure.

═══════════════════════════════════════════════════════════════════════════════
#}

{% macro publish_to_target() %}
    {% if var('enable_publish', true) and execute %}

        {% set sql %}
            CALL UDL_BATCH_PROCESS.PRC_DBT_PUBLISH_TO_TARGET('{{ invocation_id }}');
        {% endset %}

        {% set result = run_query(sql) %}

        {% if result and result.columns | length > 0 %}
            {% set response = result.columns[0].values()[0] %}
            {{ log("Publish to target result: " ~ response, info=true) }}
        {% endif %}

    {% endif %}
{% endmacro %}
