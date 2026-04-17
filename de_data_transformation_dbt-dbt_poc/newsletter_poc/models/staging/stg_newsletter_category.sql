{{
    config(
        materialized='table',
        schema='DBT_UDL_BATCH_PROCESS',
        tags=['newsletter_category', 'staging', 'variant_parse'],
        query_tag='dbt_stg_newsletter_category'
    )
}}

{% set full_load = var('is_full_load', false) %}
{% set entity_full_load = var('entity_specific_full_load', 'none') | upper %}
{% set is_entity_full_load = (full_load or 'NEWSLETTER_CATEGORY' in entity_full_load.split(',') or entity_full_load == 'ALL') %}

{% set category_columns %}
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
{% endset %}

WITH raw_data AS (
    SELECT
        {{ category_columns }},
        FALSE AS is_reprocess
    FROM {{ source('shared_services_staging', 'VW_ENL_NEWSLETTER_CATEGORY') }} c
    WHERE c.domain_payload::STRING IS NOT NULL
      AND c.domain_payload:id::STRING IS NOT NULL
      AND c.created_datetime <= '{{ var("data_process_end_time") }}'::TIMESTAMP_NTZ
),

reprocess_data AS (
    SELECT
        {{ category_columns }},
        TRUE AS is_reprocess
    FROM {{ source('shared_services_staging', 'ENL_NEWSLETTER_CATEGORY_ARCHIVE') }} c
    INNER JOIN DBT_UDL_BATCH_PROCESS.REPROCESS_REQUEST r
        ON  c.domain_payload:id::STRING = r.RECORD_CODE
        AND TRY_PARSE_JSON(c.header:tenant_info):accountId::STRING = r.TENANT_CODE
    WHERE r.STATUS = 'PENDING'
      AND r.ENTITY_TYPE = 'NEWSLETTER_CATEGORY'
      AND c.domain_payload::STRING IS NOT NULL
),

{% if is_entity_full_load %}
full_load_archive AS (
    SELECT
        {{ category_columns }},
        FALSE AS is_reprocess
    FROM {{ source('shared_services_staging', 'ENL_NEWSLETTER_CATEGORY_ARCHIVE') }} c
    WHERE c.domain_payload::STRING IS NOT NULL
      AND c.domain_payload:id::STRING IS NOT NULL
      AND c.created_datetime <= '{{ var("data_process_end_time") }}'::TIMESTAMP_NTZ
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
