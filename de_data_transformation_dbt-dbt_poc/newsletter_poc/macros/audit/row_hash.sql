{#
═══════════════════════════════════════════════════════════════════════════════
ROW HASH MACROS
═══════════════════════════════════════════════════════════════════════════════

Generic hash utilities for change detection and surrogate keys.

NOTE: The entity-specific hashes in staging models (stg_newsletter, etc.)
use inline MD5(CONCAT(IFNULL(...))) to exactly match the Scala pipeline.
These generic macros are for NEW models or general-purpose use.

═══════════════════════════════════════════════════════════════════════════════
#}


{% macro row_hash(columns, alias='dbt_row_hash') %}
    MD5(
        CONCAT_WS('||',
            {% for col in columns %}
            COALESCE(CAST({{ col }} AS VARCHAR), '__NULL__')
            {%- if not loop.last -%},{%- endif %}
            {% endfor %}
        )
    )::VARCHAR(32) AS {{ alias }}
{% endmacro %}


{% macro row_hash_exclude(columns, exclude_columns, alias='dbt_row_hash') %}
    {% set filtered_columns = [] %}
    {% for col in columns %}
        {% if col not in exclude_columns %}
            {% do filtered_columns.append(col) %}
        {% endif %}
    {% endfor %}
    {{ row_hash(filtered_columns, alias) }}
{% endmacro %}


{% macro hash_key(columns, alias='hash_key') %}
    MD5(
        CONCAT_WS('|',
            {% for col in columns %}
            COALESCE(CAST({{ col }} AS VARCHAR), '')
            {%- if not loop.last -%},{%- endif %}
            {% endfor %}
        )
    )::VARCHAR(32) AS {{ alias }}
{% endmacro %}
