{{
    config(
        materialized='table',
        schema='udl_batch_process',
        meta={'transformation': 'newsletter_category'}
    )
}}

{%- set batch_ctx = get_batch_context() -%}
{%- set is_full = batch_ctx['is_full_load'] -%}
{%- set batch_id = batch_ctx['batch_id'] -%}
{%- set batch_start_time = batch_ctx['batch_start_time'] -%}

{%- set sequence_id = 0 -%}
{%- if not is_full and execute -%}
    {%- set max_id_sql -%}
        SELECT COALESCE(MAX(id), 0) FROM {{ target.database }}.DBT_UDL.NEWSLETTER_CATEGORY
    {%- endset -%}
    {%- set max_id_result = run_query(max_id_sql) -%}
    {%- if max_id_result and max_id_result.rows | length > 0 -%}
        {%- set sequence_id = max_id_result.rows[0][0] -%}
    {%- endif -%}
{%- endif -%}

WITH source_data AS (
    SELECT * FROM {{ ref('int_newsletter_category_deduped') }}
),

with_ids AS (
    SELECT
        source_data.*,
        ROW_NUMBER() OVER (ORDER BY kafka_timestamp) + {{ sequence_id }} AS id
    FROM source_data
),

with_flags AS (
    SELECT
        with_ids.*,
        ROW_NUMBER() OVER (
            PARTITION BY tenant_code, code
            ORDER BY id DESC
        ) AS latest_id
    FROM with_ids
)

SELECT
    id,
    CASE
        WHEN LENGTH(tenant_code) = 18 THEN 'DS001'
        WHEN LENGTH(tenant_code) = 36 THEN 'DS002'
        ELSE NULL
    END AS data_source_code,
    tenant_code,
    staging_id,
    code,
    name,
    category_created_datetime,
    CASE WHEN latest_id = 1 THEN TRUE ELSE FALSE END AS active_flag,
    '{{ batch_start_time }}'::TIMESTAMP_NTZ AS active_date,
    CASE WHEN latest_id = 1 THEN NULL ELSE CURRENT_TIMESTAMP() END::TIMESTAMP_NTZ AS inactive_date,
    created_by,
    created_datetime::TIMESTAMP_NTZ AS created_datetime,
    updated_by,
    updated_datetime::TIMESTAMP_NTZ AS updated_datetime,
    {{ batch_id }} AS created_batch_run_id,
    updated_batch_run_id,
    hash_value
FROM with_flags
