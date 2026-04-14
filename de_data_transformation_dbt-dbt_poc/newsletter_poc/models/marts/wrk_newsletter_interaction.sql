{{
    config(
        materialized='table',
        schema='DBT_UDL_BATCH_PROCESS',
        cluster_by=['SUBSTRING(tenant_code, -5)'],
        tags=['newsletter_interaction', 'wrk', 'delta'],
        query_tag='dbt_wrk_newsletter_interaction',
        post_hook=["{{ log_model_with_row_count() }}"]
    )
}}

{% set full_load = var('is_full_load', false) %}
{% set entity_full = var('entity_specific_full_load', 'none') | upper %}
{% set is_this_full = full_load or 'NEWSLETTER_INTERACTION' in entity_full.split(',') or entity_full == 'ALL' %}

WITH source_data AS (
    SELECT * FROM {{ ref('stg_newsletter_interaction') }}
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY tenant_code, code
            ORDER BY interaction_datetime DESC
        ) AS rn
    FROM source_data
),

latest AS (
    SELECT * FROM ranked WHERE rn = 1
),

deduped AS (
    {% if is_this_full %}
    SELECT l.* FROM latest l
    {% else %}
    SELECT l.*
    FROM latest l
    LEFT JOIN {{ source('udl_published', 'NEWSLETTER_INTERACTION') }} t
        ON l.tenant_code = t.tenant_code
       AND l.code = t.code
       AND l.hash_value = t.hash_value
    WHERE t.hash_value IS NULL
    {% endif %}
),

ref_interaction AS (
    SELECT code AS interaction_type_code, identifier_shared_service
    FROM {{ ref('ref_newsletter_interaction_type') }}
    WHERE active_flag = TRUE
),

ref_delivery AS (
    SELECT code AS delivery_system_type_code, identifier_shared_service
    FROM {{ ref('ref_newsletter_delivery_system_type') }}
    WHERE active_flag = TRUE
),

ref_recipient AS (
    SELECT code AS recipient_type_code, include_followers AS ref_include_followers, identifier_shared_service
    FROM {{ ref('ref_newsletter_recipient_type') }}
    WHERE active_flag = TRUE
),

ref_click AS (
    SELECT code AS click_type_code, identifier_shared_service
    FROM {{ ref('ref_newsletter_click_type') }}
    WHERE active_flag = TRUE
),

ref_block AS (
    SELECT code AS block_type_code, identifier_shared_service
    FROM {{ ref('ref_newsletter_block_type') }}
    WHERE active_flag = TRUE
),

enriched AS (
    SELECT
        d.*,
        ri.interaction_type_code,
        rd.delivery_system_type_code,
        rr.recipient_type_code,
        rc.click_type_code,
        rb.block_type_code
    FROM deduped d
    LEFT JOIN ref_interaction ri
        ON d.ref_interaction_type = ri.identifier_shared_service
    LEFT JOIN ref_delivery rd
        ON d.ref_delivery_system_type = rd.identifier_shared_service
    LEFT JOIN ref_recipient rr
        ON d.ref_recipient_type = rr.identifier_shared_service
       AND d.include_followers = rr.ref_include_followers
    LEFT JOIN ref_click rc
        ON d.ref_click_type = rc.identifier_shared_service
    LEFT JOIN ref_block rb
        ON d.ref_block_type = rb.identifier_shared_service
),

final AS (
    SELECT
        {% if is_this_full %}
        ROW_NUMBER() OVER (ORDER BY kafka_timestamp) AS id,
        {% else %}
        ROW_NUMBER() OVER (ORDER BY kafka_timestamp)
            + COALESCE((SELECT MAX(id) FROM {{ source('udl_published', 'NEWSLETTER_INTERACTION') }}), 0) AS id,
        {% endif %}

        {{ data_source_code('tenant_code') }}                        AS data_source_code,
        COALESCE(tenant_code, 'N/A')                                 AS tenant_code,
        staging_id,
        COALESCE(code, 'N/A')                                        AS code,
        COALESCE(newsletter_code, 'N/A')                             AS newsletter_code,

        CASE
            WHEN LENGTH(tenant_code) = 18
                THEN CONCAT(tenant_code, ref_recipient_code)
            ELSE ref_recipient_code
        END                                                          AS recipient_code,

        COALESCE(interaction_type_code, 'NLIT000')                   AS interaction_type_code,
        COALESCE(delivery_system_type_code, 'NLDS000')               AS delivery_system_type_code,
        device_type_code,
        COALESCE(total_time_spent_on_intranet_hub, 0)                AS total_time_spent_on_intranet_hub,
        COALESCE(recipient_current_city, 'N/A')                      AS recipient_current_city,
        COALESCE(recipient_current_country, 'N/A')                   AS recipient_current_country,
        COALESCE(recipient_current_department, 'N/A')                AS recipient_current_department,
        COALESCE(recipient_current_location, 'N/A')                  AS recipient_current_location,
        COALESCE(recipient_current_email, 'N/A')                     AS recipient_current_email,
        interaction_datetime,
        COALESCE(recipient_type_code, 'NLRT000')                     AS recipient_type_code,
        COALESCE(block_code, 'N/A')                                  AS block_code,
        COALESCE(block_type_code, 'NLB000')                          AS block_type_code,
        COALESCE(click_type_code, 'NLC000')                          AS click_type_code,
        COALESCE(ip_address, 'N/A')                                  AS ip_address,
        COALESCE(link_clicked_page_title, 'N/A')                     AS link_clicked_page_title,
        COALESCE(link_clicked_url, 'N/A')                            AS link_clicked_url,
        COALESCE(session_id, 'N/A')                                  AS session_id,
        COALESCE(recipient_current_timezone, 'N/A')                  AS recipient_current_timezone,
        COALESCE(user_agent, 'N/A')                                  AS user_agent,

        TRUE                                                         AS active_flag,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ                           AS active_date,

        CURRENT_USER()                                               AS created_by,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ                           AS created_datetime,
        NULL::VARCHAR                                                AS updated_by,
        NULL::TIMESTAMP_NTZ                                          AS updated_datetime,

        hash_value,

        {{ audit_columns() }}
    FROM enriched
)

SELECT * FROM final
