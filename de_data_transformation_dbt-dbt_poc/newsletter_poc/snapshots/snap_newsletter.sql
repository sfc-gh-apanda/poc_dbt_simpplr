{% snapshot snap_newsletter %}

{{
    config(
        target_database='COMMON_TENANT_DEV',
        target_schema='DBT_UDL',
        strategy='check',
        unique_key='tenant_code || \'|\' || code',
        check_cols=['hash_value', 'actual_delivery_system_type'],
        invalidate_hard_deletes=True
    )
}}

SELECT
    id,
    data_source_code,
    tenant_code,
    staging_id,
    code,
    name,
    subject,
    sender_address,
    send_as_email,
    send_as_sms,
    send_as_ms_teams_message,
    send_as_slack_message,
    send_as_intranet,
    scheduled_at,
    sent_at,
    newsletter_created_by_code,
    newsletter_updated_by_code,
    newsletter_created_datetime,
    newsletter_updated_datetime,
    status_code,
    category_code,
    template_code,
    theme_code,
    is_archived,
    send_as_timezone_aware_schedule,
    reply_to_email_address,
    recipient_info,
    recipient_type_code,
    is_deleted,
    deleted_note,
    deleted_datetime,
    active_flag,
    active_date,
    inactive_date,
    created_by,
    created_datetime,
    updated_by,
    updated_datetime,
    hash_value,
    recipient_name,
    actual_delivery_system_type,
    dbt_loaded_at
FROM {{ ref('fct_newsletter') }}

{% endsnapshot %}
