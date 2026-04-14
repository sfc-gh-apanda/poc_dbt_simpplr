-- depends_on: {{ ref('stg_newsletter_interaction_summary') }}

{{
    config(
        materialized='table',
        schema='DBT_UDL_BATCH_PROCESS',
        cluster_by=['SUBSTRING(tenant_code, -5)'],
        tags=['newsletter', 'wrk', 'delta'],
        query_tag='dbt_wrk_newsletter',
        post_hook=["{{ log_model_with_row_count() }}"]
    )
}}

{% set full_load = var('is_full_load', false) %}
{% set entity_full = var('entity_specific_full_load', 'none') | upper %}
{% set is_this_full = full_load or 'NEWSLETTER' in entity_full.split(',') or entity_full == 'ALL' %}

WITH source_data AS (
    SELECT * FROM {{ ref('int_newsletter_joined') }}
),

deduped AS (
    {% if is_this_full %}
    SELECT sd.* FROM source_data sd
    {% else %}
    SELECT sd.*
    FROM source_data sd
    LEFT JOIN {{ source('udl_published', 'NEWSLETTER') }} t
        ON sd.tenant_code = t.tenant_code
       AND sd.code = t.code
       AND sd.hash_value = t.hash_value
    WHERE t.hash_value IS NULL
    {% endif %}
),

ref_status AS (
    SELECT code AS status_code, identifier_shared_service
    FROM {{ ref('ref_newsletter_status') }}
    WHERE active_flag = TRUE
),

ref_recipient_type AS (
    SELECT code AS recipient_type_code, include_followers AS ref_include_followers, identifier_shared_service
    FROM {{ ref('ref_newsletter_recipient_type') }}
    WHERE active_flag = TRUE
),

enriched AS (
    SELECT
        d.*,
        rs.status_code,
        rrt.recipient_type_code
    FROM deduped d
    LEFT JOIN ref_status rs
        ON d.ref_status = rs.identifier_shared_service
    LEFT JOIN ref_recipient_type rrt
        ON d.ref_recipient_type = rrt.identifier_shared_service
       AND d.include_followers = rrt.ref_include_followers
),

final AS (
    SELECT
        {% if is_this_full %}
        ROW_NUMBER() OVER (ORDER BY kafka_timestamp) AS id,
        {% else %}
        ROW_NUMBER() OVER (ORDER BY kafka_timestamp)
            + COALESCE((SELECT MAX(id) FROM {{ source('udl_published', 'NEWSLETTER_HIST') }}), 0) AS id,
        {% endif %}

        {{ data_source_code('tenant_code') }}                        AS data_source_code,
        COALESCE(tenant_code, 'N/A')                                 AS tenant_code,
        staging_id,
        COALESCE(code, 'N/A')                                        AS code,
        COALESCE(name, 'N/A')                                        AS name,
        COALESCE(subject, 'N/A')                                     AS subject,
        COALESCE(sender_address, 'N/A')                              AS sender_address,
        COALESCE(send_as_email, FALSE)                               AS send_as_email,
        COALESCE(send_as_sms, FALSE)                                 AS send_as_sms,
        COALESCE(send_as_ms_teams_message, FALSE)                    AS send_as_ms_teams_message,
        COALESCE(send_as_slack_message, FALSE)                       AS send_as_slack_message,
        COALESCE(send_as_intranet, FALSE)                            AS send_as_intranet,
        scheduled_at,
        sent_at,

        COALESCE(
            CASE
                WHEN LENGTH(tenant_code) = 18
                    THEN CONCAT(tenant_code, ref_newsletter_created_by_code)
                ELSE ref_newsletter_created_by_code
            END,
            'N/A'
        )                                                            AS newsletter_created_by_code,

        COALESCE(
            CASE
                WHEN LENGTH(tenant_code) = 18
                    THEN CONCAT(tenant_code, ref_newsletter_updated_by_code)
                ELSE ref_newsletter_updated_by_code
            END,
            'N/A'
        )                                                            AS newsletter_updated_by_code,

        newsletter_created_datetime,
        newsletter_updated_datetime,
        COALESCE(status_code, 'NLS000')                              AS status_code,
        COALESCE(category_code, 'N/A')                               AS category_code,
        COALESCE(template_code, 'N/A')                               AS template_code,
        COALESCE(theme_code, 'N/A')                                  AS theme_code,
        COALESCE(is_archived, FALSE)                                 AS is_archived,
        COALESCE(send_as_timezone_aware_schedule, FALSE)             AS send_as_timezone_aware_schedule,
        COALESCE(reply_to_email_address, 'N/A')                     AS reply_to_email_address,
        recipient_info,
        COALESCE(recipient_type_code, 'NLRT000')                     AS recipient_type_code,
        COALESCE(is_deleted, FALSE)                                  AS is_deleted,
        deleted_note,
        deleted_datetime,

        TRUE                                                         AS active_flag,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ                           AS active_date,
        NULL::TIMESTAMP_NTZ                                          AS inactive_date,

        CURRENT_USER()                                               AS created_by,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ                           AS created_datetime,
        NULL::VARCHAR                                                AS updated_by,
        NULL::TIMESTAMP_NTZ                                          AS updated_datetime,

        hash_value,
        recipient_name,
        actual_delivery_system_type,

        {{ audit_columns() }}
    FROM enriched
)

SELECT * FROM final
