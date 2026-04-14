{{
    config(
        materialized='table',
        schema='DBT_UDL_BATCH_PROCESS',
        tags=['newsletter_category', 'wrk', 'delta'],
        query_tag='dbt_wrk_newsletter_category',
        post_hook=["{{ log_model_with_row_count() }}"]
    )
}}

{% set full_load = var('is_full_load', false) %}
{% set entity_full = var('entity_specific_full_load', 'none') | upper %}
{% set is_this_full = full_load or 'NEWSLETTER_CATEGORY' in entity_full.split(',') or entity_full == 'ALL' %}

WITH source_data AS (
    SELECT * FROM {{ ref('stg_newsletter_category') }}
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY tenant_code, code
            ORDER BY category_created_datetime DESC
        ) AS rn
    FROM source_data
),

latest AS (
    SELECT * FROM ranked WHERE rn = 1
),

deduped AS (
    {% if is_this_full %}
    SELECT l.* FROM latest l
    {% else %}
    SELECT l.*
    FROM latest l
    LEFT JOIN {{ source('udl_published', 'NEWSLETTER_CATEGORY') }} t
        ON l.tenant_code = t.tenant_code
       AND l.code = t.code
       AND l.hash_value = t.hash_value
    WHERE t.hash_value IS NULL
    {% endif %}
),

final AS (
    SELECT
        {% if is_this_full %}
        ROW_NUMBER() OVER (ORDER BY kafka_timestamp) AS id,
        {% else %}
        ROW_NUMBER() OVER (ORDER BY kafka_timestamp)
            + COALESCE((SELECT MAX(id) FROM {{ source('udl_published', 'NEWSLETTER_CATEGORY') }}), 0) AS id,
        {% endif %}

        {{ data_source_code('tenant_code') }}                        AS data_source_code,
        COALESCE(tenant_code, 'N/A')                                 AS tenant_code,
        staging_id,
        COALESCE(code, 'N/A')                                        AS code,
        COALESCE(name, 'N/A')                                        AS name,
        category_created_datetime,

        TRUE                                                         AS active_flag,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ                           AS active_date,
        NULL::TIMESTAMP_NTZ                                          AS inactive_date,

        CURRENT_USER()                                               AS created_by,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ                           AS created_datetime,
        NULL::VARCHAR                                                AS updated_by,
        NULL::TIMESTAMP_NTZ                                          AS updated_datetime,

        hash_value,

        {{ audit_columns() }}
    FROM deduped
)

SELECT * FROM final
