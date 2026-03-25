{{
    config(
        materialized='table',
        schema='udl_batch_process',
        meta={'transformation': 'newsletter_category'}
    )
}}

SELECT
    id,
    data_source_code,
    COALESCE(tenant_code, 'N/A') AS tenant_code,
    staging_id,
    code,
    name,
    category_created_datetime,
    active_flag,
    active_date,
    inactive_date,
    created_by,
    created_datetime,
    updated_by,
    updated_datetime,
    created_batch_run_id,
    updated_batch_run_id,
    hash_value
FROM {{ ref('shared_stg_newsletter_category') }}
