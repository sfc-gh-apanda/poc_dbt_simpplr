{{
    config(
        materialized='ephemeral',
        meta={'transformation': 'newsletter_category'}
    )
}}

{%- set is_full = is_full_load('newsletter_category') -%}

WITH source_data AS (
    SELECT * FROM {{ ref('int_newsletter_category_latest') }}
)

{%- if not is_full %},
existing_hashes AS (
    SELECT DISTINCT hash_value
    FROM {{ target.database }}.DBT_UDL.NEWSLETTER_CATEGORY
    WHERE active_flag = TRUE
      AND CONCAT(tenant_code, code) IN (
          SELECT CONCAT(tenant_code, code) FROM source_data
      )
),

deduped AS (
    SELECT s.*
    FROM source_data s
    LEFT JOIN existing_hashes e ON s.hash_value = e.hash_value
    WHERE e.hash_value IS NULL
)
{%- else %},
deduped AS (
    SELECT * FROM source_data
)
{%- endif %}

SELECT
    code,
    tenant_code,
    staging_id,
    name,
    category_created_datetime,
    kafka_timestamp,
    created_datetime,
    updated_datetime,
    created_by,
    updated_by,
    updated_batch_run_id,
    hash_value
FROM deduped
