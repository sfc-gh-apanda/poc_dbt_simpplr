{{
    config(
        materialized='view',
        schema='simpplr_dbt_staging',
        tags=['newsletter', 'staging', 'aggregation'],
        query_tag='dbt_stg_newsletter_interaction_summary'
    )
}}

WITH interaction_info AS (
    SELECT
        TRY_PARSE_JSON(c.header:tenant_info):accountId::STRING  AS int_tenant_code,
        c.domain_payload:newsletter:id::STRING                  AS int_newsletter_code,
        CASE
            WHEN c.domain_payload:delivery_system_type::STRING = 'email'    THEN 'Email'
            WHEN c.domain_payload:delivery_system_type::STRING = 'sms'      THEN 'SMS'
            WHEN c.domain_payload:delivery_system_type::STRING = 'msTeams'  THEN 'Teams'
            WHEN c.domain_payload:delivery_system_type::STRING = 'intranet' THEN 'Intranet'
            WHEN c.domain_payload:delivery_system_type::STRING = 'slack'    THEN 'Slack'
        END AS int_ref_delivery_system_type
    FROM {{ source('shared_services_staging', 'vw_enl_newsletter_interaction') }} c
    WHERE c.domain_payload::STRING IS NOT NULL
      AND c.domain_payload:id::STRING IS NOT NULL
    GROUP BY int_tenant_code, int_newsletter_code, int_ref_delivery_system_type
)

SELECT
    LISTAGG(int_ref_delivery_system_type, ', ') AS actual_delivery_system_type,
    int_tenant_code,
    int_newsletter_code
FROM interaction_info
GROUP BY int_tenant_code, int_newsletter_code
