{{
    config(
        materialized='ephemeral',
        meta={'transformation': 'newsletter_category'}
    )
}}

{%- set batch_ctx = get_batch_context() -%}
{%- set is_full = is_full_load('newsletter_category') -%}
{%- set watermark_start = get_watermark_start('newsletter_category') -%}
{%- set watermark_end = batch_ctx['data_process_end_time'] -%}

WITH staging_data AS (
    SELECT
        c.domain_payload:id::STRING AS code,
        TRY_PARSE_JSON(c.header:tenant_info):accountId::STRING AS tenant_code,
        c.id::NUMBER AS staging_id,
        c.domain_payload:name::STRING AS name,
        TO_TIMESTAMP(c.domain_payload:created_at::STRING) AS category_created_datetime,
        c.kafka_timestamp AS kafka_timestamp,
        CURRENT_TIMESTAMP() AS created_datetime,
        NULL::TIMESTAMP_NTZ AS updated_datetime,
        CURRENT_USER() AS created_by,
        NULL::STRING AS updated_by,
        NULL::NUMBER AS updated_batch_run_id,
        MD5(CONCAT(
            IFNULL(c.domain_payload:id::STRING, ''),
            IFNULL(TRY_PARSE_JSON(c.header:tenant_info):accountId::STRING, ''),
            IFNULL(c.domain_payload:name::STRING, ''),
            IFNULL(TO_VARCHAR(TO_TIMESTAMP(c.domain_payload:created_at::STRING)), '2000-01-01 00:00:00')
        )) AS hash_value
    FROM {{ source('shared_services_staging', 'vw_enl_newsletter_category') }} c
    WHERE c.domain_payload::STRING IS NOT NULL
      AND c.domain_payload:id::STRING IS NOT NULL
      AND c.created_datetime >= '{{ watermark_start }}'::TIMESTAMP_NTZ
      AND c.created_datetime <= '{{ watermark_end }}'::TIMESTAMP_NTZ
)

{%- if is_full %},
archive_data AS (
    SELECT
        c.domain_payload:id::STRING AS code,
        TRY_PARSE_JSON(c.header:tenant_info):accountId::STRING AS tenant_code,
        c.id::NUMBER AS staging_id,
        c.domain_payload:name::STRING AS name,
        TO_TIMESTAMP(c.domain_payload:created_at::STRING) AS category_created_datetime,
        c.kafka_timestamp AS kafka_timestamp,
        CURRENT_TIMESTAMP() AS created_datetime,
        NULL::TIMESTAMP_NTZ AS updated_datetime,
        CURRENT_USER() AS created_by,
        NULL::STRING AS updated_by,
        NULL::NUMBER AS updated_batch_run_id,
        MD5(CONCAT(
            IFNULL(c.domain_payload:id::STRING, ''),
            IFNULL(TRY_PARSE_JSON(c.header:tenant_info):accountId::STRING, ''),
            IFNULL(c.domain_payload:name::STRING, ''),
            IFNULL(TO_VARCHAR(TO_TIMESTAMP(c.domain_payload:created_at::STRING)), '2000-01-01 00:00:00')
        )) AS hash_value
    FROM {{ source('shared_services_staging', 'enl_newsletter_category_archive') }} c
    WHERE c.domain_payload::STRING IS NOT NULL
      AND c.domain_payload:id::STRING IS NOT NULL
      AND c.created_datetime >= '{{ watermark_start }}'::TIMESTAMP_NTZ
      AND c.created_datetime <= '{{ watermark_end }}'::TIMESTAMP_NTZ
)
SELECT * FROM staging_data
UNION ALL
SELECT * FROM archive_data
{%- else %}
SELECT * FROM staging_data
{%- endif %}
