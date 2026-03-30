package com.simpplr.sql.shared

trait NewsletterSql {
  def NEWSLETTER_DATA_COLLECTION_SQL_SHARED: String = """ SELECT
                                                        |c.domain_payload:id::STRING AS code,
                                                        |try_parse_json(c.header:tenant_info):accountId::STRING AS tenant_code,
                                                        |c.ID::NUMBER AS staging_id,
                                                        |c.header_id::NUMBER AS header_id,
                                                        |CONCAT(staging_id::String, code, tenant_code) AS delta_join_condition,
                                                        |c.domain_payload:recipients::VARIANT AS recipient_info,
                                                        |c.domain_payload:recipients[0]:type::STRING AS ref_recipient_type,
                                                        |IFF(c.domain_payload:recipients[0]:include_followers::BOOLEAN IS NULL, FALSE, c.domain_payload:recipients[0]:include_followers::BOOLEAN) AS include_followers,
                                                        |c.domain_payload:name::STRING AS name,
                                                        |c.domain_payload:subject::STRING AS subject,
                                                        |CASE WHEN c.domain_payload:channels:email:type::STRING = 'email' THEN TRUE ELSE FALSE END AS send_as_email,
                                                        |c.domain_payload:channels:email:sender_address::STRING AS sender_address,
                                                        |CASE WHEN c.domain_payload:channels:sms:type::STRING = 'sms' THEN TRUE ELSE FALSE END AS send_as_sms,
                                                        |CASE WHEN c.domain_payload:channels:msTeams:type::STRING = 'msTeams' THEN TRUE ELSE FALSE END AS send_as_ms_teams_message,
                                                        |CASE WHEN c.domain_payload:channels:slack:type::STRING = 'slack' THEN TRUE ELSE FALSE END AS send_as_slack_message,
                                                        |CASE WHEN c.domain_payload:channels:intranet:type::STRING = 'intranet' THEN TRUE ELSE FALSE END AS send_as_intranet,
                                                        |TO_TIMESTAMP(c.domain_payload:send_at::STRING) AS scheduled_at,
                                                        |TO_TIMESTAMP(c.domain_payload:sent_at::STRING) AS sent_at,
                                                        |c.domain_payload:creator_id::STRING AS ref_newsletter_created_by_code,
                                                        |c.domain_payload:modifier_id::STRING AS ref_newsletter_updated_by_code,
                                                        |TO_TIMESTAMP(c.domain_payload:created_at::STRING) AS newsletter_created_datetime,
                                                        |TO_TIMESTAMP(c.domain_payload:modified_at::STRING) AS newsletter_updated_datetime,
                                                        |c.domain_payload:status::STRING AS ref_status,
                                                        |c.domain_payload:category:id::STRING AS category_code,
                                                        |c.domain_payload:template:id::STRING AS template_code,
                                                        |c.domain_payload:theme:id::STRING AS theme_code,
                                                        |c.domain_payload:is_archived::BOOLEAN AS is_archived,
                                                        |c.domain_payload:send_as_timezone_aware_schedule::BOOLEAN AS send_as_timezone_aware_schedule,
                                                        |c.domain_payload:reply_to_address::STRING AS reply_to_email_address,
                                                        |CASE WHEN UPPER(c.type::STRING) = 'NEWSLETTER_DELETED' THEN TRUE ELSE FALSE END AS is_deleted,
                                                        |CASE WHEN is_deleted THEN 'newsletter got deleted' ELSE NULL END AS deleted_note,
                                                        |CASE WHEN is_deleted THEN newsletter_updated_datetime ELSE NULL END AS deleted_datetime,
                                                        |c.kafka_timestamp AS kafka_timestamp,
                                                        |CURRENT_TIMESTAMP() AS created_datetime,
                                                        |NULL AS updated_datetime,
                                                        |CURRENT_USER() AS created_by,
                                                        |NULL AS updated_by,
                                                        |NULL AS updated_batch_run_id,
                                                        |MD5( CONCAT(
                                                        |IFNULL(code, ''),
                                                        |IFNULL(tenant_code, ''),
                                                        |IFNULL(name, ''),
                                                        |IFNULL(subject, ''),
                                                        |IFNULL(sender_address, ''),
                                                        |IFNULL(send_as_email, FALSE),
                                                        |IFNULL(send_as_sms, FALSE),
                                                        |IFNULL(send_as_ms_teams_message, FALSE),
                                                        |IFNULL(send_as_slack_message, FALSE),
                                                        |IFNULL(send_as_intranet, FALSE),
                                                        |IFNULL(scheduled_at, '2000-01-01 00:00:00'),
                                                        |IFNULL(sent_at, '2000-01-01 00:00:00'),
                                                        |IFNULL(ref_newsletter_created_by_code, ''),
                                                        |IFNULL(ref_newsletter_updated_by_code, ''),
                                                        |IFNULL(newsletter_created_datetime, '2000-01-01 00:00:00'),
                                                        |IFNULL(newsletter_updated_datetime, '2000-01-01 00:00:00'),
                                                        |IFNULL(ref_status, ''),
                                                        |IFNULL(category_code, ''),
                                                        |IFNULL(template_code, ''),
                                                        |IFNULL(theme_code, ''),
                                                        |IFNULL(is_archived, FALSE),
                                                        |IFNULL(send_as_timezone_aware_schedule, FALSE),
                                                        |IFNULL(reply_to_email_address, ''),
                                                        |IFNULL(is_deleted, FALSE),
                                                        |IFNULL(deleted_note, ''),
                                                        |IFNULL(deleted_datetime, '2000-01-01 00:00:00'),
                                                        |IFNULL(recipient_info, TO_VARIANT([]))
                                                        |)
                                                        |) AS hash_value
                                                        |FROM shared_services_staging.vw_enl_newsletter c
                                                        |WHERE c.domain_payload::STRING IS NOT NULL AND code IS NOT NULL """.stripMargin

  def NEWSLETTER_RECIPIENT_DETAILS_SQL_SHARED:String = """ SELECT
                                                         |LISTAGG(fv.value:name::STRING, ', ') AS recipient_name ,
                                                         |nl.id AS recp_staging_id,
                                                         |nl.domain_payload:id::STRING AS recp_code,
                                                         |TRY_PARSE_JSON(nl.header:tenant_info):accountId::STRING AS recp_tenant_code,
                                                         |CONCAT(recp_staging_id::STRING, recp_code, recp_tenant_code) AS recp_join_condition
                                                         |FROM shared_services_staging.vw_enl_newsletter nl,
                                                         |LATERAL FLATTEN(input => nl.domain_payload:recipients::VARIANT, OUTER => TRUE) AS fv
                                                         |WHERE filter_condition
                                                         |GROUP BY recp_tenant_code, recp_code, recp_staging_id """.stripMargin
  def NEWSLETTER_INTERACTION_SQL_SHARED: String = """ WITH interaction_info AS(
                                                    |SELECT
                                                    |TRY_PARSE_JSON(c.header:tenant_info):accountId::STRING AS int_tenant_code,
                                                    |c.domain_payload:newsletter:id::STRING AS int_newsletter_code,
                                                    |CASE WHEN c.domain_payload:delivery_system_type::STRING = 'email' THEN 'Email'
                                                    | WHEN c.domain_payload:delivery_system_type::STRING = 'sms' THEN 'SMS'
                                                    | WHEN c.domain_payload:delivery_system_type::STRING = 'msTeams' THEN 'Teams'
                                                    | WHEN c.domain_payload:delivery_system_type::STRING = 'intranet' THEN 'Intranet'
                                                    | WHEN c.domain_payload:delivery_system_type::STRING = 'slack' THEN 'Slack' END
                                                    |AS int_ref_delivery_system_type
                                                    |FROM shared_services_staging.vw_enl_newsletter_interaction c
                                                    |WHERE filter_condition
                                                    |GROUP BY int_tenant_code, int_newsletter_code, int_ref_delivery_system_type)
                                                    |SELECT LISTAGG(int_ref_delivery_system_type, ', ') AS actual_delivery_system_type,
                                                    |int_tenant_code, int_newsletter_code
                                                    |FROM interaction_info
                                                    |GROUP BY int_tenant_code, int_newsletter_code """.stripMargin

  def TARGET_NEWSLETTER_DATA_SQL_SHARED: String =
    """ SELECT n.* FROM udl.newsletter n
      |WHERE EXISTS
      |(SELECT 1 FROM udl_batch_process.stg_newsletter_interaction_only_code_data d
      |WHERE d.int_newsletter_code = n.code AND d.int_tenant_code = n.tenant_code ) """.stripMargin
}
