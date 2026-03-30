{{
    config(
        materialized='table',
        schema='UDL',
        tags=['newsletter_category', 'staging', 'variant_parse'],
        query_tag='dbt_stg_newsletter_category'
    )
}}

{% set full_load = var('is_full_load', false) %}
{% set entity_full_load = var('entity_specific_full_load', 'none') %}
{% set is_entity_full_load = (full_load or entity_full_load == 'newsletter_category') %}

WITH raw_data AS (
    SELECT
        c.id::NUMBER                                                    AS staging_id,
        c.header_id::NUMBER                                             AS header_id,
        c.domain_payload:id::STRING                                     AS code,
        TRY_PARSE_JSON(c.header:tenant_info):accountId::STRING          AS tenant_code,

        c.domain_payload:name::STRING                                   AS name,
        TO_TIMESTAMP(c.domain_payload:created_at::STRING)               AS category_created_datetime,

        c.kafka_timestamp,
        c.created_datetime,

        MD5(CONCAT(
            IFNULL(code, ''),
            IFNULL(tenant_code, ''),
            IFNULL(name, ''),
            IFNULL(category_created_datetime, '{{ var("default_null_timestamp") }}')
        )) AS hash_value

    FROM {{ source('shared_services_staging', 'VW_ENL_NEWSLETTER_CATEGORY') }} c
    WHERE c.domain_payload::STRING IS NOT NULL
      AND c.domain_payload:id::STRING IS NOT NULL
      AND c.created_datetime <= '{{ var("data_process_end_time") }}'::TIMESTAMP_NTZ
),

{% if is_entity_full_load %}
archive_data AS (
    SELECT
        c.id::NUMBER                                                    AS staging_id,
        c.header_id::NUMBER                                             AS header_id,
        c.domain_payload:id::STRING                                     AS code,
        TRY_PARSE_JSON(c.header:tenant_info):accountId::STRING          AS tenant_code,

        c.domain_payload:name::STRING                                   AS name,
        TO_TIMESTAMP(c.domain_payload:created_at::STRING)               AS category_created_datetime,

        c.kafka_timestamp,
        c.created_datetime,

        MD5(CONCAT(
            IFNULL(code, ''),
            IFNULL(tenant_code, ''),
            IFNULL(name, ''),
            IFNULL(category_created_datetime, '{{ var("default_null_timestamp") }}')
        )) AS hash_value

    FROM {{ source('shared_services_staging', 'ENL_NEWSLETTER_CATEGORY_ARCHIVE') }} c
    WHERE c.domain_payload::STRING IS NOT NULL
      AND c.domain_payload:id::STRING IS NOT NULL
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
