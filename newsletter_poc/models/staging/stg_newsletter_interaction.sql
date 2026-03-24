{{
    config(
        materialized='view',
        schema='simpplr_dbt_staging',
        tags=['newsletter_interaction', 'staging', 'variant_parse'],
        query_tag='dbt_stg_newsletter_interaction'
    )
}}

SELECT
    c.id::NUMBER                                                    AS staging_id,
    c.header_id::NUMBER                                             AS header_id,
    c.domain_payload:id::STRING                                     AS code,
    TRY_PARSE_JSON(c.header:tenant_info):accountId::STRING          AS tenant_code,

    c.domain_payload:newsletter:id::STRING                          AS newsletter_code,
    c.domain_payload:newsletter:recipients[0]:type::STRING          AS ref_recipient_type,
    IFF(
        c.domain_payload:recipients[0]:include_followers::BOOLEAN IS NULL,
        FALSE,
        c.domain_payload:recipients[0]:include_followers::BOOLEAN
    )                                                               AS include_followers,
    c.domain_payload:user:people_id::STRING                         AS ref_recipient_code,

    c.domain_payload:interaction::STRING                            AS ref_interaction_type,
    LOWER(c.domain_payload:delivery_system_type::STRING)            AS ref_delivery_system_type,

    c.domain_payload:seconds_on_app::NUMBER                         AS total_time_spent_on_intranet_hub,
    c.domain_payload:user:city::STRING                              AS recipient_current_city,
    c.domain_payload:user:country::STRING                           AS recipient_current_country,
    c.domain_payload:user:department::STRING                        AS recipient_current_department,
    c.domain_payload:user:location::STRING                          AS recipient_current_location,
    c.domain_payload:user:email::STRING                             AS recipient_current_email,

    CASE
        WHEN LOWER(c.domain_payload:user_agent::STRING) LIKE '%native mobile app%'     THEN 'DT001'
        WHEN LOWER(c.domain_payload:user_agent::STRING) LIKE '%desktop app%'           THEN 'DT004'
        WHEN LOWER(c.domain_payload:user_agent::STRING) LIKE '%mobile app%'
         AND LOWER(c.domain_payload:user_agent::STRING) NOT LIKE '%native_mobile_app%' THEN 'DT002'
        WHEN LOWER(c.domain_payload:user_agent::STRING) LIKE '%windows phone%'
          OR LOWER(c.domain_payload:user_agent::STRING) LIKE '%iphone%'
          OR LOWER(c.domain_payload:user_agent::STRING) LIKE '%ipad%'
          OR LOWER(c.domain_payload:user_agent::STRING) LIKE '%android%'
          OR LOWER(c.domain_payload:user_agent::STRING) LIKE '%symbianos%'             THEN 'DT003'
        WHEN LOWER(c.domain_payload:user_agent::STRING) LIKE '%macintosh%'
          OR LOWER(c.domain_payload:user_agent::STRING) LIKE '%windows nt%'
          OR LOWER(c.domain_payload:user_agent::STRING) LIKE '%x11%'                   THEN 'DT005'
        ELSE 'DT000'
    END                                                             AS device_type_code,

    TO_TIMESTAMP(c.domain_payload:timestamp::STRING)                AS interaction_datetime,
    c.domain_payload:block_id::STRING                               AS block_code,
    c.domain_payload:block_type::STRING                             AS ref_block_type,
    c.domain_payload:click_type::STRING                             AS ref_click_type,
    c.domain_payload:ip_address::STRING                             AS ip_address,
    c.domain_payload:title::STRING                                  AS link_clicked_page_title,
    c.domain_payload:url::STRING                                    AS link_clicked_url,
    c.domain_payload:session_id::STRING                             AS session_id,
    c.domain_payload:user:timezone_iso::STRING                      AS recipient_current_timezone,
    c.domain_payload:user_agent::STRING                             AS user_agent,

    c.kafka_timestamp,
    c.created_datetime,

    MD5(CONCAT(
        IFNULL(code, ''),
        IFNULL(tenant_code, ''),
        IFNULL(newsletter_code, ''),
        IFNULL(ref_recipient_type, ''),
        IFNULL(include_followers, FALSE),
        IFNULL(ref_recipient_code, ''),
        IFNULL(ref_interaction_type, ''),
        IFNULL(ref_delivery_system_type, ''),
        IFNULL(device_type_code, ''),
        IFNULL(total_time_spent_on_intranet_hub, 0),
        IFNULL(recipient_current_city, ''),
        IFNULL(recipient_current_country, ''),
        IFNULL(recipient_current_department, ''),
        IFNULL(recipient_current_location, ''),
        IFNULL(recipient_current_email, ''),
        IFNULL(interaction_datetime, '{{ var("default_null_timestamp") }}'),
        IFNULL(block_code, ''),
        IFNULL(ref_block_type, ''),
        IFNULL(ref_click_type, ''),
        IFNULL(ip_address, ''),
        IFNULL(link_clicked_page_title, ''),
        IFNULL(link_clicked_url, ''),
        IFNULL(session_id, ''),
        IFNULL(recipient_current_timezone, ''),
        IFNULL(user_agent, '')
    )) AS hash_value

FROM {{ source('shared_services_staging', 'vw_enl_newsletter_interaction') }} c
WHERE c.domain_payload::STRING IS NOT NULL
  AND c.domain_payload:id::STRING IS NOT NULL
