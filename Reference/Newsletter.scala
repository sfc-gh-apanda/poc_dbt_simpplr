package com.simpplr.shared

import com.simpplr.base.DataTransformationBase
import com.simpplr.configdata.PublishToSwappedTargetTable
import com.simpplr.entities.TransformationResponse
import com.simpplr.sql.CommonSql
import com.simpplr.sql.shared.NewsletterSql
import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._

class Newsletter(session: Session, name: String, source: String, target: String) extends
  DataTransformationBase(session: Session, name: String, source: String, target: String) with NewsletterSql with CommonSql{

  val isFullLoadRunningForBatch: Boolean = batchRunConfig.fullLoadFlag;
  var (isFullLoadRunningForProcess: Boolean, dataProcessStartTime) = setConfigForSpecificEntityFullLoad(target, isFullLoadRunningForBatch)
/***
 * *
 * @return TransformationResponse
 */
def transform(): TransformationResponse = {
  try {
    
    val sequenceIdCurrentValue: Long = if (isFullLoadRunningForBatch || isFullLoadRunningForProcess) 0 else (session.sql(MAX_ID_OF_TABLE.format(target + "_hist")).collect()(0).getLong(0))

      val deltaDfWithoutRecipientAndInteractionInfo = {
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

      val recipientDeltaDf = {
        val recipientDeltaRawDf: DataFrame = getCustomDeltaFromStagingTable(NEWSLETTER_RECIPIENT_DETAILS_SQL_SHARED)
        if (isFullLoadRunningForBatch || isFullLoadRunningForProcess) {
          val deltaFromArchiveTable: DataFrame = getCustomDeltaFromArchiveTable(NEWSLETTER_RECIPIENT_DETAILS_SQL_SHARED, source)
          recipientDeltaRawDf.unionByName(deltaFromArchiveTable)
        }
        else {
          recipientDeltaRawDf
        }
      }

      val interactionDeltaDf = {
        val interactionDeltaRawDf: DataFrame = getCustomDeltaFromStagingTable(NEWSLETTER_INTERACTION_SQL_SHARED)
        if (isFullLoadRunningForBatch || isFullLoadRunningForProcess) {
          val deltaFromArchiveTable: DataFrame = getCustomDeltaFromArchiveTable(NEWSLETTER_INTERACTION_SQL_SHARED, "shared_services_staging.vw_enl_newsletter_interaction")
          interactionDeltaRawDf.unionByName(deltaFromArchiveTable)
        }
        else {
          interactionDeltaRawDf
        }
      }

    val deltaDF = deltaDfWithoutRecipientAndInteractionInfo.join(recipientDeltaDf, deltaDfWithoutRecipientAndInteractionInfo.col("delta_join_condition")===recipientDeltaDf.col("recp_join_condition"), "left")
      .join(interactionDeltaDf, deltaDfWithoutRecipientAndInteractionInfo.col("tenant_code")===interactionDeltaDf.col("int_tenant_code") && deltaDfWithoutRecipientAndInteractionInfo.col("code")===interactionDeltaDf.col("int_newsletter_code"), "left")
      .select(deltaDfWithoutRecipientAndInteractionInfo.col("*"),
        recipientDeltaDf.col("recipient_name"),
        interactionDeltaDf.col("actual_delivery_system_type")
      ).drop("delta_join_condition").drop("header_id")

    val uniqueDeltaData = removeDuplicateFromDeltaDfByTenantCode(deltaDF,target, isFullLoadRunningForBatch || isFullLoadRunningForProcess)

    var recordToSaveInBatchProcessTable = assignPrimaryKeyIdAndActiveFlag(uniqueDeltaData,
      sequenceIdCurrentValue, "kafka_timestamp")

    //Assign value for data_source_id here in dataframe
    recordToSaveInBatchProcessTable = recordToSaveInBatchProcessTable
      .withColumn("data_source_code", when(length(col("tenant_code")) === 18, lit("DS001")).when(length(col("tenant_code")) === 36, lit("DS002")))
      .withColumn("created_batch_run_id", lit(processRunConfigObj.batchRunId))
      .withColumn("active_date", lit(batchRunConfig.batchStartTime))
      .withColumn("newsletter_created_by_code", when(length(col("tenant_code")) === 18, concat( col("tenant_code"), col("ref_newsletter_created_by_code"))).when(length(col("tenant_code")) === 36, col("ref_newsletter_created_by_code")))
      .withColumn("newsletter_updated_by_code", when(length(col("tenant_code")) === 18, concat( col("tenant_code"), col("ref_newsletter_updated_by_code"))).when(length(col("tenant_code")) === 36, col("ref_newsletter_updated_by_code")))
      .drop(Seq(
        "ref_newsletter_created_by_code", "ref_newsletter_updated_by_code"
      ))

    recordToSaveInBatchProcessTable.write.mode(SaveMode.Overwrite)
      .saveAsTable("udl_batch_process.shared_stg_newsletter")

    val stgDf = session.table("udl_batch_process.shared_stg_newsletter")
      .select(col("*"))

    totalRecordsToProcessCount = stgDf.count()

    val refNewsletterStatusDf = getRefDataForSharedService("udl.ref_newsletter_status")
    val refNewsletterRecipientTypeDf = session.table("udl.ref_newsletter_recipient_type").filter(col("active_flag") === true)
      .select(
        col("id").as("ref_id"),
        col("code").as("ref_code"),
        col("include_followers").as("ref_include_followers"),
        col("identifier_shared_service")
      )

    val referenceDataSource = stgDf
      .join(
        refNewsletterStatusDf, stgDf.col("ref_status") === refNewsletterStatusDf.col("identifier_shared_service"), "left")
      .join(
        refNewsletterRecipientTypeDf, (stgDf.col("ref_recipient_type") === refNewsletterRecipientTypeDf.col("identifier_shared_service") and stgDf.col("include_followers") === refNewsletterRecipientTypeDf.col("ref_include_followers")), "left")
      .select(
        stgDf.col("*"),
        refNewsletterStatusDf.col("ref_code").as("status_code"),
        refNewsletterRecipientTypeDf.col("ref_code").as("recipient_type_code")
      )
      .dropDuplicates()

    val stgDfAfterDroppingCodeColumn = referenceDataSource.drop(Seq(
      "ref_status","ref_recipient_type","include_followers"
    ))

    val stgDfAfterDroppingCodeColumnAndReplaceNa = stgDfAfterDroppingCodeColumn.na.fill(Map(
      "tenant_code" -> "N/A",
      "status_code" -> "NLS000",
      "newsletter_created_by_code" -> "N/A",
      "newsletter_updated_by_code" -> "N/A",
      "category_code" -> "N/A",
      "template_code" -> "N/A",
      "theme_code" -> "N/A",
      "recipient_type_code" -> "NLRT000"
    ))

    stgDfAfterDroppingCodeColumnAndReplaceNa.write.mode(SaveMode.Append).option("columnOrder", "name")
      .saveAsTable("udl_batch_process.wrk_newsletter")

//    Logic to update delivery_system_type for the records from udl.newsletter

    if(!(isFullLoadRunningForBatch || isFullLoadRunningForProcess)) {
      val codesToUpdateInUDL = interactionDeltaDf.join(stgDfAfterDroppingCodeColumnAndReplaceNa, stgDfAfterDroppingCodeColumnAndReplaceNa.col("code") === interactionDeltaDf.col("int_newsletter_code")
        && stgDfAfterDroppingCodeColumnAndReplaceNa.col("tenant_code") === interactionDeltaDf.col("int_tenant_code"), "left_anti")
        .select(interactionDeltaDf.col("int_tenant_code"), interactionDeltaDf.col("int_newsletter_code"))

      codesToUpdateInUDL.write.mode(SaveMode.Overwrite)
        .saveAsTable("udl_batch_process.stg_newsletter_interaction_only_code_data")

      val udlNewsletterDf = session.sql(TARGET_NEWSLETTER_DATA_SQL_SHARED).drop(Seq("actual_delivery_system_type", "updated_datetime", "updated_batch_run_id"))
      val udlNewsletterDataWithActualDeliverySystemInfo = udlNewsletterDf.join(interactionDeltaDf, udlNewsletterDf.col("tenant_code") === interactionDeltaDf.col("int_tenant_code") && udlNewsletterDf.col("code") === interactionDeltaDf.col("int_newsletter_code"), "left")
        .select(udlNewsletterDf.col("*"),
          interactionDeltaDf.col("actual_delivery_system_type"),
          lit(current_timestamp()).as("updated_datetime"),
          lit(batchRunConfig.batchRunId).as("updated_batch_run_id")
        )
      udlNewsletterDataWithActualDeliverySystemInfo.write.mode(SaveMode.Append).option("columnOrder", "name")
        .saveAsTable("udl_batch_process.wrk_newsletter")
    }

    addRecordsFromTargetWithActiveFlagFalseIntoWorkTableByTenantCode(session, target, "udl_batch_process.wrk_newsletter", "tenant_code,code", isFullLoadRunningForBatch || isFullLoadRunningForProcess)

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
  val sql = s"$NEWSLETTER_DATA_COLLECTION_SQL_SHARED " +
    s"AND (c.created_datetime >='${dataProcessStartTime}' AND c.created_datetime <='${batchRunConfig.dataProcessEndTime}' ) "
  session.sql(sql)
}

  def getCustomDeltaFromStagingTable(SQL_STRING : String): DataFrame = {
    val sql = SQL_STRING.replace("filter_condition", s" (created_datetime >='${dataProcessStartTime}' AND created_datetime <='${batchRunConfig.dataProcessEndTime}' ) ")
    session.sql(sql)
  }

  def getDeltaFromArchiveTable(): DataFrame = {
    val sql = s"$NEWSLETTER_DATA_COLLECTION_SQL_SHARED " +
      s"AND (c.created_datetime >='${dataProcessStartTime}' AND c.created_datetime <='${batchRunConfig.dataProcessEndTime}' ) "
    val archiveDeltaSql = sql.replace(source, source.replace("vw_", "") + "_archive")
    session.sql(archiveDeltaSql)
  }

  def getCustomDeltaFromArchiveTable(SQL_STRING: String, Source: String): DataFrame = {
    val sql = SQL_STRING.replace("filter_condition", s" (created_datetime >='${dataProcessStartTime}' AND created_datetime <='${batchRunConfig.dataProcessEndTime}' ) ")
    val archiveDeltaSql = sql.replace(Source, Source.replace("vw_", "") + "_archive")
    session.sql(archiveDeltaSql)
  }

  def assignPrimaryKeyIdAndActiveFlag(stagingDf: DataFrame, sequenceIdCurrentValue: Long,
                                      sortOrderColumnName: String): DataFrame = {
    val windowSpec  = Window.orderBy(functions.col(sortOrderColumnName))
    val deltaSourceDataWithIdsDf = stagingDf
      .withColumn("id", functions.row_number().over(windowSpec)+sequenceIdCurrentValue)
      .drop(sortOrderColumnName)

    val windowSpecLatest  = Window.partitionBy(col("tenant_code"),col("code")).orderBy(col("id").desc)
    val latestRecordsDf = deltaSourceDataWithIdsDf
      .withColumn("latest_id", functions.row_number().over(windowSpecLatest))
      .select(
        deltaSourceDataWithIdsDf.col("*"),
        when(col("latest_id") === lit(1), lit(true))
          .otherwise(lit(false)).as("active_flag"),
        when(col("latest_id") === lit(1), lit(null) )
          .otherwise(functions.current_timestamp()).as("inactive_date")
      )
    latestRecordsDf
  }
}