package com.simpplr.shared

import com.simpplr.base.DataTransformationBase
import com.simpplr.configdata.PublishToSwappedTargetTable
import com.simpplr.entities.TransformationResponse
import com.simpplr.sql.CommonSql
import com.simpplr.sql.shared.NewsletterCategorySql
import com.snowflake.snowpark._
import com.snowflake.snowpark.functions._

class NewsletterCategory(session: Session, name: String, source: String, target: String) extends
DataTransformationBase(session: Session, name: String, source: String, target: String) with NewsletterCategorySql with CommonSql{

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
      // Rank rows within each partition by interaction_datetime
      val windowSpec = Window.partitionBy(col("tenant_code"),col("code")).orderBy(col("category_created_datetime").desc)
      val filteredDeltaDF = deltaDF.drop("header_id").withColumn("rank", row_number().over(windowSpec)).where(col("rank") === 1).drop("rank")

    val uniqueDeltaData = removeDuplicateFromDeltaDfByTenantCode(filteredDeltaDF, target, isFullLoadRunningForBatch || isFullLoadRunningForProcess)

    var recordToSaveInBatchProcessTable = assignPrimaryKeyIdAndActiveFlag(uniqueDeltaData,
      sequenceIdCurrentValue, "kafka_timestamp")

    recordToSaveInBatchProcessTable = recordToSaveInBatchProcessTable
      .withColumn("data_source_code", when(length(col("tenant_code")) === 18, lit("DS001")).when(length(col("tenant_code")) === 36, lit("DS002")))
      .withColumn("created_batch_run_id", lit(processRunConfigObj.batchRunId))
      .withColumn("active_date", lit(batchRunConfig.batchStartTime))

    recordToSaveInBatchProcessTable.write.mode(SaveMode.Overwrite)
      .saveAsTable("udl_batch_process.shared_stg_newsletter_category")

    val stgDf = session.table("udl_batch_process.shared_stg_newsletter_category")
      .select(col("*"))

    totalRecordsToProcessCount = stgDf.count()

    val referenceDataSource = stgDf.dropDuplicates()

    val stgDfAfterDroppingCodeColumnAndReplaceNa = referenceDataSource.na.fill(Map(
      "tenant_code" -> "N/A"
    ))

    stgDfAfterDroppingCodeColumnAndReplaceNa.write.mode(SaveMode.Append).option("columnOrder", "name")
      .saveAsTable("udl_batch_process.wrk_newsletter_category")

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
    val sql = s"$NEWSLETTER_CATEGORY_DATA_COLLECTION_SQL_SHARED " +
      s"AND (c.created_datetime >='${dataProcessStartTime}' AND c.created_datetime <='${batchRunConfig.dataProcessEndTime}' ) "
    session.sql(sql)
  }

  def getDeltaFromArchiveTable(): DataFrame = {
    val sql = s"$NEWSLETTER_CATEGORY_DATA_COLLECTION_SQL_SHARED " +
      s"AND (c.created_datetime >='${dataProcessStartTime}' AND c.created_datetime <='${batchRunConfig.dataProcessEndTime}' ) "
    val archiveDeltaSql = sql.replace(source, source.replace("vw_", "") + "_archive")
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