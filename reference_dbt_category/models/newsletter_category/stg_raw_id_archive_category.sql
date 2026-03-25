{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        schema='udl_batch_process',
        unique_key=['batch_run_id', 'source_entity_name', 'staging_id'],
        meta={'transformation': 'newsletter_category'}
    )
}}

{%- set batch_id = get_batch_id() -%}

SELECT
    {{ batch_id }} AS batch_run_id,
    'vw_enl_newsletter_category' AS source_entity_name,
    staging_id,
    CURRENT_TIMESTAMP() AS archived_at
FROM {{ ref('stg_newsletter_category') }}
WHERE staging_id IS NOT NULL
