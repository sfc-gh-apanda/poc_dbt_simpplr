{{
    config(
        materialized='table',
        schema='DBT_UDL_BATCH_PROCESS',
        tags=['newsletter', 'staging', 'flatten'],
        query_tag='dbt_stg_newsletter_recipient'
    )
}}

{% set full_load = var('is_full_load', false) %}
{% set entity_full_load = var('entity_specific_full_load', 'none') | upper %}
{% set is_entity_full_load = (full_load or 'NEWSLETTER' in entity_full_load.split(',') or entity_full_load == 'ALL') %}

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

reprocess_data AS (
    SELECT
        {{ recipient_columns }}
    FROM {{ source('shared_services_staging', 'ENL_NEWSLETTER_ARCHIVE') }} nl
    INNER JOIN DBT_UDL_BATCH_PROCESS.REPROCESS_REQUEST r
        ON  nl.domain_payload:id::STRING = r.RECORD_CODE
        AND TRY_PARSE_JSON(nl.header:tenant_info):accountId::STRING = r.TENANT_CODE,
         LATERAL FLATTEN(input => nl.domain_payload:recipients::VARIANT, OUTER => TRUE) AS fv
    WHERE r.STATUS = 'PENDING'
      AND r.ENTITY_TYPE = 'NEWSLETTER'
      AND nl.domain_payload::STRING IS NOT NULL
    GROUP BY recp_tenant_code, recp_code, recp_staging_id
),

{% if is_entity_full_load %}
full_load_archive AS (
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
    SELECT * FROM full_load_archive
)
{% else %}
combined AS (
    SELECT * FROM raw_data
    UNION ALL
    SELECT * FROM reprocess_data
)
{% endif %}

SELECT * FROM combined
