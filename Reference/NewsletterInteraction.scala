package com.simpplr.shared

import com.simpplr.base.DataTransformationBase
import com.simpplr.configdata.PublishToSwappedTargetTable
import com.simpplr.entities.TransformationResponse
import com.simpplr.sql.CommonSql
import com.simpplr.sql.shared.NewsletterInteractionSql
import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._

class NewsletterInteraction(session: Session, name: String, source: String, target: String) extends
DataTransformationBase(session: Session, name: String, source: String, target: String) with NewsletterInteractionSql with CommonSql{

  val isFullLoadRunningForBatch: Boolean = batchRunConfig.fullLoadFlag;
  var (isFullLoadRunningForProcess: Boolean, dataProcessStartTime) = setConfigForSpecificEntityFullLoad(target, isFullLoadRunningForBatch)
/***
 * *
 * @return TransformationResponse
 */
def transform(): TransformationResponse = {
  try {
    
    val sequenceIdCurrentValue: Long = if (isFullLoadRunningForBatch || isFullLoadRunningForProcess) 0 else (session.sql(MAX_ID_OF_TABLE.format(target)).collect()(0).getLong(0))


      val deltaDF = {
        val rawDelta: DataFrame = getDeltaFromStagingTable()
        prepareRawTableIdsToArchive(rawDelta, source, "udl_batch_process.stg_employee_newsletter_data_archive", processRunConfigObj.batchRunId)

        if (isFullLoadRunningForBatch || isFullLoadRunningForProcess) {
          val deltaFromArchiveTable: DataFrame = getDeltaFromArchiveTable()
          rawDelta.unionByName(deltaFromArchiveTable)
        }
        else {
          rawDelta
        }
      }

    val deltaData = removeDuplicateFromDeltaDfByTenantCode(deltaDF,target, isFullLoadRunningForBatch || isFullLoadRunningForProcess)

    // Rank rows within each partition by interaction_datetime
    val windowSpecZeus = Window.partitionBy(col("tenant_code"),col("code")).orderBy(col("interaction_datetime").desc)
    val rankedUniqueDeltaData = deltaData.withColumn("rank", row_number().over(windowSpecZeus))

      // Select only the latest record in each partition
      val uniqueDeltaData = rankedUniqueDeltaData.where(col("rank") === 1).drop("rank").drop("header_id")

    val uniqueDeltaDataWithId = assignPrimaryKeyIdAndActiveFlag(uniqueDeltaData,
      sequenceIdCurrentValue, "kafka_timestamp")

    //Assign value for data_source_id here in dataframe
    val recordToSaveInBatchProcessTable = uniqueDeltaDataWithId
      .withColumn("data_source_code", when(length(col("tenant_code")) === 18, lit("DS001")).when(length(col("tenant_code")) === 36, lit("DS002")))
      .withColumn("created_batch_run_id", lit(processRunConfigObj.batchRunId))
      .withColumn("active_date", lit(batchRunConfig.batchStartTime))
      .withColumn("active_flag", lit("true"))
      .withColumn("recipient_code", when(length(col("tenant_code")) === 18, concat( col("tenant_code"), col("ref_recipient_code"))).when(length(col("tenant_code")) === 36, col("ref_recipient_code")))
      .drop(Seq(
        "ref_recipient_code"
      ))

    recordToSaveInBatchProcessTable.write.mode(SaveMode.Overwrite)
      .saveAsTable("udl_batch_process.shared_stg_newsletter_interaction")

    val stgDf = session.table("udl_batch_process.shared_stg_newsletter_interaction")
      .select(col("*"))

    totalRecordsToProcessCount = stgDf.count()

    val refNewsletterInteractionTypeDf = getRefDataForSharedService("udl.ref_newsletter_interaction_type")
    val refNewsletterDeliverySystemTypeDf = getRefDataForSharedService("udl.ref_newsletter_delivery_system_type")
    val refNewsletterClickTypeDf = getRefDataForSharedService("udl.ref_newsletter_click_type")
    val refNewsletterBlockTypeDf = getRefDataForSharedService("udl.ref_newsletter_block_type")
    val refNewsletterRecipientTypeDf = session.table("udl.ref_newsletter_recipient_type").filter(col("active_flag") === true)
      .select(
        col("id").as("ref_id"),
        col("code").as("ref_code"),
        col("include_followers").as("ref_include_followers"),
        col("identifier_shared_service")
      )

    val referenceDataSource = stgDf
      .join(
        refNewsletterRecipientTypeDf, (stgDf.col("ref_recipient_type") === refNewsletterRecipientTypeDf.col("identifier_shared_service") and stgDf.col("include_followers") === refNewsletterRecipientTypeDf.col("ref_include_followers")), "left")
      .join(
        refNewsletterInteractionTypeDf, stgDf.col("ref_interaction_type") === refNewsletterInteractionTypeDf.col("identifier_shared_service"), "left")
      .join(
        refNewsletterDeliverySystemTypeDf, stgDf.col("ref_delivery_system_type") === refNewsletterDeliverySystemTypeDf.col("identifier_shared_service"), "left")
      .join(
        refNewsletterClickTypeDf, stgDf.col("ref_click_type") === refNewsletterClickTypeDf.col("identifier_shared_service"), "left")
      .join(
        refNewsletterBlockTypeDf, stgDf.col("ref_block_type") === refNewsletterBlockTypeDf.col("identifier_shared_service"), "left")
      .select(
        stgDf.col("*"),
        refNewsletterRecipientTypeDf.col("ref_code").as("recipient_type_code"),
        refNewsletterInteractionTypeDf.col("ref_code").as("interaction_type_code"),
        refNewsletterDeliverySystemTypeDf.col("ref_code").as("delivery_system_type_code"),
        refNewsletterClickTypeDf.col("ref_code").as("click_type_code"),
        refNewsletterBlockTypeDf.col("ref_code").as("block_type_code")
      )
      .dropDuplicates()

    val replaceNullAndEmptyWithUndefined = referenceDataSource
      .withColumn("recipient_current_city", when(col("recipient_current_city").is_null or col("recipient_current_city") === "", lit("Undefined"))
        .otherwise(col("recipient_current_city")))
      .withColumn("recipient_current_email", when(col("recipient_current_email").is_null or col("recipient_current_email") === "", lit("Undefined"))
        .otherwise(col("recipient_current_email")))
      .withColumn("recipient_current_country", when(col("recipient_current_country").is_null or col("recipient_current_country") === "", lit("Undefined"))
        .otherwise(col("recipient_current_country")))
      .withColumn("recipient_current_department", when(col("recipient_current_department").is_null or col("recipient_current_department") === "", lit("Undefined"))
        .otherwise(col("recipient_current_department")))
      .withColumn("recipient_current_location", when(col("recipient_current_location").is_null or col("recipient_current_location") === "", lit("Undefined"))
        .otherwise(col("recipient_current_location")))
      .withColumn("recipient_current_timezone", when(col("recipient_current_timezone").is_null or col("recipient_current_timezone") === "", lit("Undefined"))
        .otherwise(col("recipient_current_timezone")))

    val stgDfAfterDroppingCodeColumn = replaceNullAndEmptyWithUndefined.drop(Seq(
      "ref_recipient_type","ref_interaction_type", "ref_delivery_system_type", "include_followers", "ref_click_type", "ref_block_type"
    ))

    val stgDfAfterDroppingCodeColumnAndReplaceNa = stgDfAfterDroppingCodeColumn.na.fill(Map(
      "tenant_code" -> "N/A",
      "recipient_code" -> "N/A",
      "recipient_type_code" -> "NLRT000",
      "interaction_type_code" -> "NLIT000",
      "delivery_system_type_code" -> "NLDS000",
      "click_type_code" -> "NLC000",
      "block_type_code" -> "NLB000",
      "block_code" -> "N/A"
    ))

    stgDfAfterDroppingCodeColumnAndReplaceNa.write.mode(SaveMode.Append).option("columnOrder", "name")
      .saveAsTable("udl_batch_process.wrk_newsletter_interaction")
      PublishToSwappedTargetTable.initiateWrkToSwappedTargetTableProcess(session, target, "shared",  isFullLoadRunningForBatch || isFullLoadRunningForProcess)
      saveProgress(totalRecordsToProcessCount = totalRecordsToProcessCount)
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        handleException(e)
    }
  }

  def getDeltaFromStagingTable(): DataFrame = {
    val sql = s"$NEWSLETTER_INTERACTION_DATA_COLLECTION_SQL_SHARED " +
      s"AND (c.created_datetime >='${dataProcessStartTime}' AND c.created_datetime <='${batchRunConfig.dataProcessEndTime}' ) "
    session.sql(sql)
  }

  def getDeltaFromArchiveTable(): DataFrame = {
    val sql = s"$NEWSLETTER_INTERACTION_DATA_COLLECTION_SQL_SHARED " +
      s"AND (c.created_datetime >='${dataProcessStartTime}' AND c.created_datetime <='${batchRunConfig.dataProcessEndTime}' ) "
    val archiveDeltaSql = sql.replace(source, source.replace("vw_", "") + "_archive")
    session.sql(archiveDeltaSql)
  }

  def assignPrimaryKeyIdAndActiveFlag(stagingDf: DataFrame, sequenceIdCurrentValue: Long,
                                      sortOrderColumnName: String): DataFrame = {
    val windowSpec = Window.orderBy(functions.col(sortOrderColumnName))
    val uniqueDf = stagingDf.dropDuplicates()
    if ((isFullLoadRunningForBatch || isFullLoadRunningForProcess)) {
      val deltaSourceDataWithIdsDf = uniqueDf
        .withColumn("id", functions.row_number().over(windowSpec) + sequenceIdCurrentValue)
        .drop(sortOrderColumnName)
      deltaSourceDataWithIdsDf
    } else {
      val deltaSourceDataWithIdsDf = uniqueDf
        .withColumn("id_temp", functions.row_number().over(windowSpec) + sequenceIdCurrentValue)
        .drop(sortOrderColumnName)

      val recordsFromTargetTableToBeUpdatedDf = session.table("udl.newsletter_interaction")
        .filter(concat(col("tenant_code") ,col("code")) in (deltaSourceDataWithIdsDf.select(concat(col("tenant_code") ,col("code")))))
        .select(col("code").as("target_code"), col("tenant_code").as("target_tenant_code"), col("id").as("target_id"))

      val finalUpdatedDf = deltaSourceDataWithIdsDf.join(recordsFromTargetTableToBeUpdatedDf, deltaSourceDataWithIdsDf.col("code") === recordsFromTargetTableToBeUpdatedDf.col("target_code")
        && deltaSourceDataWithIdsDf.col("tenant_code") === recordsFromTargetTableToBeUpdatedDf.col("target_tenant_code")
        , "left")
        .select(
          deltaSourceDataWithIdsDf.col("*"),
          when(recordsFromTargetTableToBeUpdatedDf.col("target_id").is_null, deltaSourceDataWithIdsDf.col("id_temp")).otherwise(recordsFromTargetTableToBeUpdatedDf.col("target_id")).as("id"))
      finalUpdatedDf.drop("id_temp")
    }

  }
}