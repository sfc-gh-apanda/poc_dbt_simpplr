{{
    config(
        materialized='ephemeral'
    )
}}

WITH newsletter AS (
    SELECT * FROM {{ ref('stg_newsletter') }}
),

recipient AS (
    SELECT * FROM {{ ref('stg_newsletter_recipient') }}
),

interaction_summary AS (
    SELECT * FROM {{ ref('stg_newsletter_interaction_summary') }}
),

joined AS (
    SELECT
        nl.*,
        r.recipient_name,
        ias.actual_delivery_system_type
    FROM newsletter nl
    LEFT JOIN recipient r
        ON nl.delta_join_condition = r.recp_join_condition
    LEFT JOIN interaction_summary ias
        ON nl.tenant_code = ias.int_tenant_code
       AND nl.code = ias.int_newsletter_code
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY tenant_code, code
            ORDER BY kafka_timestamp DESC
        ) AS rn
    FROM joined
)

SELECT * FROM ranked
