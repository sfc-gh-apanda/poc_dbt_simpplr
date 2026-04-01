{{
    config(
        materialized='ephemeral',
        tags=['newsletter', 'staging', 'aggregation'],
        query_tag='dbt_stg_newsletter_interaction_summary'
    )
}}

{#
  Reads from stg_newsletter_interaction (already-parsed data) instead of
  scanning VW_ENL_NEWSLETTER_INTERACTION a second time. The delivery system
  type mapping (email→Email, sms→SMS, etc.) mirrors the Scala NEWSLETTER_INTERACTION_SQL_SHARED.
  ref_delivery_system_type from stg_newsletter_interaction is already LOWER()-ed,
  so the CASE matches on lowercase.
#}

WITH interaction_info AS (
    SELECT DISTINCT
        tenant_code                     AS int_tenant_code,
        newsletter_code                 AS int_newsletter_code,
        CASE ref_delivery_system_type
            WHEN 'email'    THEN 'Email'
            WHEN 'sms'      THEN 'SMS'
            WHEN 'msteams'  THEN 'Teams'
            WHEN 'intranet' THEN 'Intranet'
            WHEN 'slack'    THEN 'Slack'
        END                             AS int_ref_delivery_system_type
    FROM {{ ref('stg_newsletter_interaction') }}
)

SELECT
    LISTAGG(int_ref_delivery_system_type, ', ') AS actual_delivery_system_type,
    int_tenant_code,
    int_newsletter_code
FROM interaction_info
GROUP BY int_tenant_code, int_newsletter_code
