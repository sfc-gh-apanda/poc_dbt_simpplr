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

SELECT 'fct_newsletter' AS model, COUNT(*) AS row_count
FROM {{ ref('fct_newsletter') }}

UNION ALL

SELECT 'fct_newsletter_interaction', COUNT(*)
FROM {{ ref('fct_newsletter_interaction') }}

UNION ALL

SELECT 'fct_newsletter_category', COUNT(*)
FROM {{ ref('fct_newsletter_category') }}
