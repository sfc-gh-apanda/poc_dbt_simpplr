{% macro data_source_code(tenant_code_column) %}
    CASE
        WHEN LENGTH({{ tenant_code_column }}) = 18 THEN 'DS001'
        WHEN LENGTH({{ tenant_code_column }}) = 36 THEN 'DS002'
        ELSE NULL
    END
{% endmacro %}
