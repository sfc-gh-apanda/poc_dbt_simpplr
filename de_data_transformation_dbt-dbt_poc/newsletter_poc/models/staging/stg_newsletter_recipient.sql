{{
    config(
        materialized='table',
        schema='UDL',
        tags=['newsletter', 'staging', 'flatten'],
        query_tag='dbt_stg_newsletter_recipient'
    )
}}

{% set full_load = var('is_full_load', false) %}
{% set entity_full_load = var('entity_specific_full_load', 'none') %}
{% set is_entity_full_load = (full_load or entity_full_load == 'newsletter') %}

{% set recipient_columns %}
    LISTAGG(fv.value:name::STRING, ', ') AS recipient_name,
    nl.id                                AS recp_staging_id,
    nl.domain_payload:id::STRING         AS recp_code,
    TRY_PARSE_JSON(nl.header:tenant_info):accountId::STRING AS recp_tenant_code,
    CONCAT(recp_staging_id::STRING, recp_code, recp_tenant_code) AS recp_join_condition
{% endset %}

{% set recipient_where %}
    nl.domain_payload::STRING IS NOT NULL
    AND nl.domain_payload:id::STRING IS NOT NULL
    AND nl.created_datetime >= '{{ var("data_process_start_time") }}'::TIMESTAMP_NTZ
    AND nl.created_datetime <= '{{ var("data_process_end_time") }}'::TIMESTAMP_NTZ
{% endset %}

WITH raw_data AS (
    SELECT
        {{ recipient_columns }}
    FROM {{ source('shared_services_staging', 'VW_ENL_NEWSLETTER') }} nl,
         LATERAL FLATTEN(input => nl.domain_payload:recipients::VARIANT, OUTER => TRUE) AS fv
    WHERE {{ recipient_where }}
    GROUP BY recp_tenant_code, recp_code, recp_staging_id
),

{% if is_entity_full_load %}
archive_data AS (
    SELECT
        {{ recipient_columns }}
    FROM {{ source('shared_services_staging', 'ENL_NEWSLETTER_ARCHIVE') }} nl,
         LATERAL FLATTEN(input => nl.domain_payload:recipients::VARIANT, OUTER => TRUE) AS fv
    WHERE {{ recipient_where }}
    GROUP BY recp_tenant_code, recp_code, recp_staging_id
),

combined AS (
    SELECT * FROM raw_data
    UNION ALL
    SELECT * FROM archive_data
)
{% else %}
combined AS (
    SELECT * FROM raw_data
)
{% endif %}

SELECT * FROM combined
