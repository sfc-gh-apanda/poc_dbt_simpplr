package com.simpplr.sql.shared

trait NewsletterCategorySql {
  def NEWSLETTER_CATEGORY_DATA_COLLECTION_SQL_SHARED: String = """ SELECT
                                                                 |c.domain_payload:id::STRING AS code,
                                                                 |try_parse_json(c.header:tenant_info):accountId::STRING AS tenant_code,
                                                                 |c.ID::NUMBER AS staging_id,
                                                                 |c.header_id::NUMBER AS header_id,
                                                                 |c.domain_payload:name::STRING AS name,
                                                                 |TO_TIMESTAMP(c.domain_payload:created_at::STRING) AS category_created_datetime,
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
                                                                 |IFNULL(category_created_datetime, '2000-01-01 00:00:00')
                                                                 |)
                                                                 |) AS hash_value
                                                                 |FROM shared_services_staging.vw_enl_newsletter_category c
                                                                 |WHERE c.domain_payload::STRING IS NOT NULL AND code IS NOT NULL """.stripMargin
}
