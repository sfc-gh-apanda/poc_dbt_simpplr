{{
    config(
        materialized='view',
        schema='DBT_UDL',
        tags=['sentinel', 'publish', 'archive'],
        query_tag='dbt_pipeline_complete',
        post_hook=[
            "{{ publish_to_target() }}",
            "{{ archive_raw_data() }}"
        ]
    )
}}

SELECT 'wrk_newsletter' AS model, COUNT(*) AS row_count
FROM {{ ref('wrk_newsletter') }}

UNION ALL

SELECT 'wrk_newsletter_interaction', COUNT(*)
FROM {{ ref('wrk_newsletter_interaction') }}

UNION ALL

SELECT 'wrk_newsletter_category', COUNT(*)
FROM {{ ref('wrk_newsletter_category') }}
