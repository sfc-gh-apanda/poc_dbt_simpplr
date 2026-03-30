{{
    config(
        materialized='table',
        schema='UDL',
        tags=['newsletter', 'staging', 'aggregation'],
        query_tag='dbt_stg_newsletter_interaction_summary'
    )
}}

{% set full_load = var('is_full_load', false) %}
{% set entity_full_load = var('entity_specific_full_load', 'none') %}
{% set is_entity_full_load = (full_load or entity_full_load == 'newsletter_interaction') %}

{% set summary_columns %}
    TRY_PARSE_JSON(c.header:tenant_info):accountId::STRING  AS int_tenant_code,
    c.domain_payload:newsletter:id::STRING                  AS int_newsletter_code,
    CASE
        WHEN c.domain_payload:delivery_system_type::STRING = 'email'    THEN 'Email'
        WHEN c.domain_payload:delivery_system_type::STRING = 'sms'      THEN 'SMS'
        WHEN c.domain_payload:delivery_system_type::STRING = 'msTeams'  THEN 'Teams'
        WHEN c.domain_payload:delivery_system_type::STRING = 'intranet' THEN 'Intranet'
        WHEN c.domain_payload:delivery_system_type::STRING = 'slack'    THEN 'Slack'
    END AS int_ref_delivery_system_type
{% endset %}

{% set summary_where %}
    c.domain_payload::STRING IS NOT NULL
    AND c.domain_payload:id::STRING IS NOT NULL
    AND c.created_datetime <= '{{ var("data_process_end_time") }}'::TIMESTAMP_NTZ
{% endset %}

WITH raw_interaction AS (
    SELECT
        {{ summary_columns }}
    FROM {{ source('shared_services_staging', 'VW_ENL_NEWSLETTER_INTERACTION') }} c
    WHERE {{ summary_where }}
    GROUP BY int_tenant_code, int_newsletter_code, int_ref_delivery_system_type
),

{% if is_entity_full_load %}
archive_interaction AS (
    SELECT
        {{ summary_columns }}
    FROM {{ source('shared_services_staging', 'ENL_NEWSLETTER_INTERACTION_ARCHIVE') }} c
    WHERE {{ summary_where }}
    GROUP BY int_tenant_code, int_newsletter_code, int_ref_delivery_system_type
),

interaction_info AS (
    SELECT * FROM raw_interaction
    UNION ALL
    SELECT * FROM archive_interaction
)
{% else %}
interaction_info AS (
    SELECT * FROM raw_interaction
)
{% endif %}

SELECT
    LISTAGG(int_ref_delivery_system_type, ', ') AS actual_delivery_system_type,
    int_tenant_code,
    int_newsletter_code
FROM interaction_info
GROUP BY int_tenant_code, int_newsletter_code
