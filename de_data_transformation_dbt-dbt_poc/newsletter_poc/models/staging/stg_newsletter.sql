{{
    config(
        materialized='table',
        schema='UDL',
        tags=['newsletter', 'staging', 'variant_parse'],
        query_tag='dbt_stg_newsletter'
    )
}}

{% set full_load = var('is_full_load', false) %}
{% set entity_full_load = var('entity_specific_full_load', 'none') %}
{% set is_entity_full_load = (full_load or entity_full_load == 'newsletter') %}

{% set newsletter_columns %}
    c.id::NUMBER                                                    AS staging_id,
    c.header_id::NUMBER                                             AS header_id,
    c.domain_payload:id::STRING                                     AS code,
    TRY_PARSE_JSON(c.header:tenant_info):accountId::STRING          AS tenant_code,
    CONCAT(staging_id::STRING, code, tenant_code)                   AS delta_join_condition,

    c.domain_payload:recipients::VARIANT                            AS recipient_info,
    c.domain_payload:recipients[0]:type::STRING                     AS ref_recipient_type,
    IFF(
        c.domain_payload:recipients[0]:include_followers::BOOLEAN IS NULL,
        FALSE,
        c.domain_payload:recipients[0]:include_followers::BOOLEAN
    )                                                               AS include_followers,

    c.domain_payload:name::STRING                                   AS name,
    c.domain_payload:subject::STRING                                AS subject,

    CASE WHEN c.domain_payload:channels:email:type::STRING = 'email'       THEN TRUE ELSE FALSE END AS send_as_email,
    c.domain_payload:channels:email:sender_address::STRING          AS sender_address,
    CASE WHEN c.domain_payload:channels:sms:type::STRING = 'sms'           THEN TRUE ELSE FALSE END AS send_as_sms,
    CASE WHEN c.domain_payload:channels:msTeams:type::STRING = 'msTeams'   THEN TRUE ELSE FALSE END AS send_as_ms_teams_message,
    CASE WHEN c.domain_payload:channels:slack:type::STRING = 'slack'       THEN TRUE ELSE FALSE END AS send_as_slack_message,
    CASE WHEN c.domain_payload:channels:intranet:type::STRING = 'intranet' THEN TRUE ELSE FALSE END AS send_as_intranet,

    TO_TIMESTAMP(c.domain_payload:send_at::STRING)                  AS scheduled_at,
    TO_TIMESTAMP(c.domain_payload:sent_at::STRING)                  AS sent_at,

    c.domain_payload:creator_id::STRING                             AS ref_newsletter_created_by_code,
    c.domain_payload:modifier_id::STRING                            AS ref_newsletter_updated_by_code,
    TO_TIMESTAMP(c.domain_payload:created_at::STRING)               AS newsletter_created_datetime,
    TO_TIMESTAMP(c.domain_payload:modified_at::STRING)              AS newsletter_updated_datetime,

    c.domain_payload:status::STRING                                 AS ref_status,
    c.domain_payload:category:id::STRING                            AS category_code,
    c.domain_payload:template:id::STRING                            AS template_code,
    c.domain_payload:theme:id::STRING                               AS theme_code,
    c.domain_payload:is_archived::BOOLEAN                           AS is_archived,
    c.domain_payload:send_as_timezone_aware_schedule::BOOLEAN       AS send_as_timezone_aware_schedule,
    c.domain_payload:reply_to_address::STRING                       AS reply_to_email_address,

    CASE WHEN UPPER(c.type::STRING) = 'NEWSLETTER_DELETED' THEN TRUE ELSE FALSE END AS is_deleted,
    CASE WHEN is_deleted THEN 'newsletter got deleted'   ELSE NULL END              AS deleted_note,
    CASE WHEN is_deleted THEN newsletter_updated_datetime ELSE NULL END             AS deleted_datetime,

    c.kafka_timestamp,
    c.created_datetime,

    MD5(CONCAT(
        IFNULL(code, ''),
        IFNULL(tenant_code, ''),
        IFNULL(name, ''),
        IFNULL(subject, ''),
        IFNULL(sender_address, ''),
        IFNULL(send_as_email, FALSE),
        IFNULL(send_as_sms, FALSE),
        IFNULL(send_as_ms_teams_message, FALSE),
        IFNULL(send_as_slack_message, FALSE),
        IFNULL(send_as_intranet, FALSE),
        IFNULL(scheduled_at, '{{ var("default_null_timestamp") }}'),
        IFNULL(sent_at, '{{ var("default_null_timestamp") }}'),
        IFNULL(ref_newsletter_created_by_code, ''),
        IFNULL(ref_newsletter_updated_by_code, ''),
        IFNULL(newsletter_created_datetime, '{{ var("default_null_timestamp") }}'),
        IFNULL(newsletter_updated_datetime, '{{ var("default_null_timestamp") }}'),
        IFNULL(ref_status, ''),
        IFNULL(category_code, ''),
        IFNULL(template_code, ''),
        IFNULL(theme_code, ''),
        IFNULL(is_archived, FALSE),
        IFNULL(send_as_timezone_aware_schedule, FALSE),
        IFNULL(reply_to_email_address, ''),
        IFNULL(is_deleted, FALSE),
        IFNULL(deleted_note, ''),
        IFNULL(deleted_datetime, '{{ var("default_null_timestamp") }}'),
        IFNULL(recipient_info, TO_VARIANT(ARRAY_CONSTRUCT()))
    )) AS hash_value
{% endset %}

WITH raw_data AS (
    SELECT
        {{ newsletter_columns }}
    FROM {{ source('shared_services_staging', 'VW_ENL_NEWSLETTER') }} c
    WHERE c.domain_payload::STRING IS NOT NULL
      AND c.domain_payload:id::STRING IS NOT NULL
      AND c.created_datetime >= '{{ var("data_process_start_time") }}'::TIMESTAMP_NTZ
      AND c.created_datetime <= '{{ var("data_process_end_time") }}'::TIMESTAMP_NTZ
),

{% if is_entity_full_load %}
archive_data AS (
    SELECT
        {{ newsletter_columns }}
    FROM {{ source('shared_services_staging', 'ENL_NEWSLETTER_ARCHIVE') }} c
    WHERE c.domain_payload::STRING IS NOT NULL
      AND c.domain_payload:id::STRING IS NOT NULL
      AND c.created_datetime >= '{{ var("data_process_start_time") }}'::TIMESTAMP_NTZ
      AND c.created_datetime <= '{{ var("data_process_end_time") }}'::TIMESTAMP_NTZ
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
