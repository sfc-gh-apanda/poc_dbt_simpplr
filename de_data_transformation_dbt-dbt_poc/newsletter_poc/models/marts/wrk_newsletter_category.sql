{{
    config(
        materialized='incremental',
        unique_key=['tenant_code', 'code'],
        incremental_strategy='merge',
        on_schema_change='fail',
        merge_exclude_columns=['id'],
        schema='DBT_UDL',
        tags=['newsletter_category', 'wrk', 'merge'],
        query_tag='dbt_wrk_newsletter_category',
        post_hook=["{{ log_model_with_row_count() }}"]
    )
}}

WITH source_data AS (
    SELECT * FROM {{ ref('stg_newsletter_category') }}
    {% if is_incremental() %}
    WHERE created_datetime >= (
        SELECT COALESCE(MAX(dbt_loaded_at), '2000-01-01'::TIMESTAMP_NTZ)
        FROM {{ this }}
    )
    {% endif %}
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

{% if is_incremental() %}
deduped AS (
    SELECT l.*
    FROM latest l
    LEFT JOIN {{ this }} t
        ON l.tenant_code = t.tenant_code
       AND l.code = t.code
       AND l.hash_value = t.hash_value
    WHERE t.hash_value IS NULL
),
{% else %}
deduped AS (
    SELECT * FROM latest
),
{% endif %}

final AS (
    SELECT
        {% if is_incremental() %}
            ROW_NUMBER() OVER (ORDER BY kafka_timestamp)
                + (SELECT COALESCE(MAX(id), 0) FROM {{ this }}) AS id,
        {% else %}
            ROW_NUMBER() OVER (ORDER BY kafka_timestamp) AS id,
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
