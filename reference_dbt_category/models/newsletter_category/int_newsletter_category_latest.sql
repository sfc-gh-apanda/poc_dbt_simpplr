{{
    config(
        materialized='ephemeral',
        meta={'transformation': 'newsletter_category'}
    )
}}

WITH ranked AS (
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
        hash_value,
        ROW_NUMBER() OVER (
            PARTITION BY tenant_code, code
            ORDER BY category_created_datetime DESC
        ) AS rank_id
    FROM {{ ref('stg_newsletter_category') }}
)

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
FROM ranked
WHERE rank_id = 1
