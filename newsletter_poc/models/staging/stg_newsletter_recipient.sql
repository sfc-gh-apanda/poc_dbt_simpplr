{{
    config(
        materialized='view',
        schema='simpplr_dbt_staging',
        tags=['newsletter', 'staging', 'flatten'],
        query_tag='dbt_stg_newsletter_recipient'
    )
}}

SELECT
    LISTAGG(fv.value:name::STRING, ', ') AS recipient_name,
    nl.id                                AS recp_staging_id,
    nl.domain_payload:id::STRING         AS recp_code,
    TRY_PARSE_JSON(nl.header:tenant_info):accountId::STRING AS recp_tenant_code,
    CONCAT(recp_staging_id::STRING, recp_code, recp_tenant_code) AS recp_join_condition
FROM {{ source('shared_services_staging', 'vw_enl_newsletter') }} nl,
     LATERAL FLATTEN(input => nl.domain_payload:recipients::VARIANT, OUTER => TRUE) AS fv
WHERE nl.domain_payload::STRING IS NOT NULL
  AND nl.domain_payload:id::STRING IS NOT NULL
GROUP BY recp_tenant_code, recp_code, recp_staging_id
