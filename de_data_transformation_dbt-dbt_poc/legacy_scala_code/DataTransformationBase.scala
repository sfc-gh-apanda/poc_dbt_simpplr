package com.simpplr.base

import com.simpplr.common.Constants
import com.simpplr.configdata.{BatchRunDetails, ProcessRunStats, PublishToSwappedTargetTable}
import com.simpplr.entities.{BatchRun, ProcessRunConfig, RemoveDuplicateFromDeltaRequest, TransformationResponse}
import com.snowflake.snowpark.functions.{col, concat, current_date, current_timestamp, current_user, date_trunc, lit, max, month, row_number, to_date, when, year, substring}
import com.snowflake.snowpark.{DataFrame, Session}
import com.snowflake.snowpark._

import java.sql.Date
import java.time.LocalDateTime
import java.time.{Instant, LocalDate, ZoneOffset}


abstract class DataTransformationBase(session: Session, name: String, source: String, target: String) extends Constants{
  var batchRunConfig: BatchRun = BatchRunDetails.getStartAndEndTimeOfBatch(session, target, name)
  if (batchRunConfig == null) {
    handleException(new Exception("Data not available in batch run table"))
  }
  var totalRecordsToProcessCount: Long = 0

  val processRunConfigObj: ProcessRunConfig = new ProcessRunConfig {
    override var batchRunId: Int = batchRunConfig.batchRunId
    override var processName: String = name
    override var processStartTime: String = batchRunConfig.dataProcessStartTime
    override var processEndTime: String = batchRunConfig.dataProcessEndTime
    override var processSource: String = source
    override var processTarget: String = target
    override var processStartedTime:LocalDateTime = LocalDateTime.now()
    override var processNotes: String = ""
    override var noOfRecordsProcessed: Long = 0
    override var errorMessage: String = ""
    override var processStatus: String = ""
  }


  def transform(): TransformationResponse

  def getDeltaFromStagingTable(): DataFrame

  /***
   * This method removes the duplicates from delta by comparing the delta with the target table
   * @param request: RemoveDuplicateFromDeltaRequest
   * @return rawDataDfAfterDuplicateEliminated DataFrame
   */
  def removeDuplicateFromDelta(request: RemoveDuplicateFromDeltaRequest): DataFrame = {
    val deltaDf = request.deltaDf
    val targetData = session.table(request.targetTable)
      .filter(
        (col(request.targetColumn) in deltaDf.select(col(request.sourceColumn))) &&
          (col(request.sourceActiveFlagColumn) === true))
      .select(col(request.targetHashColumn).as(request.targetHashColumnAlias))

    val rawDataDfAfterDuplicateEliminated = deltaDf
      .join(
        targetData,
        deltaDf.col(request.targetHashColumn) === targetData.col(request.targetHashColumnAlias),"left")
      .filter(targetData.col(request.targetHashColumnAlias).is_null)
      .select(deltaDf.col("*"))
    rawDataDfAfterDuplicateEliminated
  }

  def removeDuplicateFromDeltaDf(sourceDf: DataFrame, targetTable: String, isFullLoadTrueForBatchOrProcess: Boolean): DataFrame = {
    if (isFullLoadTrueForBatchOrProcess) {
      sourceDf
    }
    else {
      val deltaDf = sourceDf
      val targetDf = session.table(targetTable)

      def hasColumn(df: DataFrame, name: String): Boolean =
        df.schema.fields.exists(f => f.name.equalsIgnoreCase(name))

      val deltaHasTenant = hasColumn(deltaDf, "tenant_code") || hasColumn(deltaDf, "ref_tenant_code")
      val deltaTenantCol = if (hasColumn(deltaDf, "tenant_code")) "tenant_code" else if (hasColumn(deltaDf, "ref_tenant_code")) "ref_tenant_code" else ""
      val deltaHasTenantAndCode = deltaHasTenant && hasColumn(deltaDf, "code")
      val targetHasTenantAndCode = hasColumn(targetDf, "tenant_code") && hasColumn(targetDf, "code")
      val targetHasActiveFlag = hasColumn(targetDf, "active_flag")

      val baseFilter = if (deltaHasTenantAndCode && targetHasTenantAndCode) {
        (concat(col("tenant_code"), col("code")) in deltaDf.select(concat(col(deltaTenantCol), col("code"))))
      } else if (hasColumn(deltaDf, "code") && hasColumn(targetDf, "code")) {
        (col("code") in deltaDf.select(col("code")))
      } else {
        // If required columns are missing, skip elimination to avoid breaking
        lit(false)
      }

      val filterWithActive = if (targetHasActiveFlag) baseFilter && (col("active_flag") === true) else baseFilter

      val targetData = targetDf
        .filter(filterWithActive)
        .select(col("hash_value").as("target_hash_value"))

      val rawDataDfAfterDuplicateEliminated = deltaDf
        .join(
          targetData,
          deltaDf.col("hash_value") === targetData.col("target_hash_value"), "left")
        .filter(targetData.col("target_hash_value").is_null)
        .select(deltaDf.col("*"))
      rawDataDfAfterDuplicateEliminated
    }
  }

  def removeDuplicateFromDeltaDfByTenantCode(sourceDf: DataFrame, targetTable: String, isFullLoadTrueForBatchOrProcess: Boolean): DataFrame = {
    if (isFullLoadTrueForBatchOrProcess) {
      sourceDf
    }
    else {
      val deltaDf = sourceDf
      val targetData = session.table(targetTable)
        .filter(
          (concat(col("tenant_code") ,col("code")) in deltaDf.select(concat(col("tenant_code") ,col("code")))) &&
            (col("active_flag") === true))
        .select(col("hash_value").as("target_hash_value"))

      val rawDataDfAfterDuplicateEliminated = deltaDf
        .join(
          targetData,
          deltaDf.col("hash_value") === targetData.col("target_hash_value"), "left")
        .filter(targetData.col("target_hash_value").is_null)
        .select(deltaDf.col("*"))
      rawDataDfAfterDuplicateEliminated
    }
  }

  def setConfigForZeusSearchEventsDataLoad(dataProcessStartTime: String) = {
    var queryOnlyInRudderstackTableVal = true
    val rudderstackSearchStaticDateVal = "2025-01-01 00:00:00.000"
    if (rudderstackSearchStaticDateVal >= dataProcessStartTime) {
      queryOnlyInRudderstackTableVal = false
    }

    (queryOnlyInRudderstackTableVal, rudderstackSearchStaticDateVal)
  }

  def setConfigForSpecificEntityFullLoad(target: String, isFullLoadRunningForBatch: Boolean) = {
    var isFullLoadRunningForProcess = false
    if(batchRunConfig.processRunningInFullLoad.trim.nonEmpty && !isFullLoadRunningForBatch){
      val lst_of_transformation = batchRunConfig.processRunningInFullLoad.split(',').map(_.trim.toLowerCase).toList
      if(lst_of_transformation.contains(target.toLowerCase())){
        batchRunConfig.dataProcessStartTime = "2000-01-01 00:00:00.000"
        isFullLoadRunningForProcess = true
      }
    }
    (isFullLoadRunningForProcess , batchRunConfig.dataProcessStartTime)
  }

  def assignPrimaryKeyIdAndActiveFlag(allStagingRecordsToProcess: DataFrame, sequenceIdCurrentValue: Long, sortOrderColumnName: String): DataFrame

  def handleException(e: Exception): TransformationResponse = {
    e.printStackTrace()
    val errorMessages = e.getMessage

    val failureMessage = FAILURE_RESPONSE_MESSAGE.format(
      name,
      source,
      target,
      batchRunConfig.batchRunId,
      batchRunConfig.dataProcessStartTime,
      batchRunConfig.dataProcessEndTime,
      totalRecordsToProcessCount
    )

    val response = new TransformationResponse{
      override var code = 400
      override var message: String = failureMessage
      override var status: String = "ERROR"
    }

    processRunConfigObj.noOfRecordsProcessed = totalRecordsToProcessCount
    processRunConfigObj.errorMessage = failureMessage
    processRunConfigObj.processStatus = "ERROR"
    processRunConfigObj.processNotes = errorMessages

    ProcessRunStats.saveProcessStatus(processRunConfigObj, session)
    response
  }

  def saveProgress(totalRecordsToProcessCount: Long): TransformationResponse  = {
    processRunConfigObj.noOfRecordsProcessed = totalRecordsToProcessCount
    processRunConfigObj.errorMessage = ""
    processRunConfigObj.processStatus = "SUCCESS"

    ProcessRunStats.saveProcessStatus(processRunConfigObj, session)
    val response = new TransformationResponse {
      override var code = 200
      override var message: String = SUCCESS_RESPONSE_MESSAGE.format(
        name,
        source,
        target,
        batchRunConfig.batchRunId,
        batchRunConfig.dataProcessStartTime,
        batchRunConfig.dataProcessEndTime,
        totalRecordsToProcessCount
      )
      override var status = "SUCCESS"
    }
    response
  }

  /**
   *
   * This method returns a dataframe of tenant_information data from UDL and Work Tables with two columns 'id' AS tenant_id and code AS tenant_inf_code.
   * @return : DataFrame
   */
  def getTenantInfoDataFromUdlAndWorkTables() :DataFrame = {
    val refTenantInfoData = session.table("udl.tenant_information")
      .select(col("id"), col("code"))
      .union(session.table("udl_batch_process.wrk_tenant_information")
        .select(col("id"), col("code"))
      )
      .groupBy(col("code")).max(col("id"))
      .select(
        col("MAX(ID)").as("tenant_id"),
        col("code").as("tenant_inf_code"))

    refTenantInfoData
  }

  /**
   *
   * @param stgDf delta data to be transformed
   * @param stgDfRefColumnName ref column name in stgDf for user data
   * @param udlOrWorkFilterColumn can either be code or additional_identifier only
   * @return DataFrame with user details for populating in stgDf [transformed_user_id, transformed_user_code, transformed_additional_identifier]
   */
  def getUserDetails(stgOrWorkTableName: String, stgDf: DataFrame, stgDfRefColumnName: String, udlOrWorkFilterColumn: String): DataFrame = {
    val wrkDf = session.table(stgOrWorkTableName)
      .filter((col(udlOrWorkFilterColumn) in stgDf.select(col(stgDfRefColumnName)))
        && (col("active_flag") === true))
      .select(
        col("id").as("transformed_user_id"),
        col("code").as("transformed_user_code"),
        col("additional_identifier").as("transformed_additional_identifier"),
        col("user_modified_datetime")
      )
    val udlDf = session.table("udl.user")
        .filter((col(udlOrWorkFilterColumn) in stgDf.select(col(stgDfRefColumnName)))
          && (col("active_flag") === true) && (!col(udlOrWorkFilterColumn) in session.table(stgOrWorkTableName)
          .select(col("code"))))
        .select(
          col("id").as("transformed_user_id"),
          col("code").as("transformed_user_code"),
          col("additional_identifier").as("transformed_additional_identifier"),
          col("user_modified_datetime")
      )
    if (batchRunConfig.fullLoadFlag) wrkDf else wrkDf.union(udlDf)
  }

  /**
   *
   * @param stgOrWorkTableName - table for stg or wrk to be used when selecting data for ref
   * @param stgDf - delta dataframe to be used for table being transformed
   * @param stgDfRefColumnName - column from delta df to be used to ref comparison code or additional_identifier
   * @param udlOrWorkFilterColumn - column from wrk or udl df to be used to ref comparison code or additional_identifier
   * @param refTargetTable - udl table name for ref table
   * @param columnName - column names to select
   * @return
   */
  def getRefCodeDataWithColumn(stgOrWorkTableName: String,
                               stgDf: DataFrame,
                               stgDfRefColumnName: String,
                               udlOrWorkFilterColumn: String,
                               refTargetTable: String, columnName: String): DataFrame = {
    val columns = columnName.split(",").map(name => col(name).as(String.format("%s_%s", "ref", name)))

    val wrkDf = session.table(stgOrWorkTableName)
      .filter((col(udlOrWorkFilterColumn) in stgDf.select(col(stgDfRefColumnName)))
        && (col("active_flag") === true))
      .select(columns)

    val udlDf = session.table(refTargetTable)
        .filter((col(udlOrWorkFilterColumn) in stgDf.select(col(stgDfRefColumnName)))
          && (col("active_flag") === true) && (!col(udlOrWorkFilterColumn) in session.table(stgOrWorkTableName)
          .select(col("code"))))
        .select(columns)
    if (batchRunConfig.fullLoadFlag) wrkDf else wrkDf.union(udlDf)
  }


  /**
   * col("id").as("ref_id"),
   * col("code").as("ref_code"),
   * col("identifier_zeus"),
   * col("identifier_core")
   *
   * @param refTableName Table name for ref tables like ref_timezone in udl schema, value should be udl.ref_timezone
   * @return
   */
  def getRefData(refTableName: String): DataFrame ={
    session.table(refTableName)
      .filter(col("active_flag") === true)
      .select(
        col("id").as("ref_id"),
        col("code").as("ref_code"),
        col("identifier_zeus"),
        col("identifier_core")
      )
  }

  def getRefDataForSharedService(refTableName: String): DataFrame = {
    session.table(refTableName)
      .filter(col("active_flag") === true)
      .select(
        col("id").as("ref_id"),
        col("code").as("ref_code"),
        col("identifier_shared_service")
      )
  }

  def addRecordsFromTargetWithActiveFlagFalseIntoWorkTable(session: Session, targetTable: String, workTable: String, filterColumn: String, isFullLoadTrueForBatchOrProcess: Boolean) = {
    if (!isFullLoadTrueForBatchOrProcess) {
      //##### Adding records from target table with active_flag false into work table
      val targetToDfUpdateDf = session.table(targetTable + "_hist")
        .filter(
          col(filterColumn) in session.table(workTable).filter(col("active_flag") === true).select(col(filterColumn))
        )
        .filter(col("active_flag") === true)
        .select(col("*"))

      val dropTargetColumnsForUpdatingDf = targetToDfUpdateDf.drop(Seq("active_flag", "inactive_date"))
      val addUpdatedColumnsInWorkTable = dropTargetColumnsForUpdatingDf
        .withColumn("active_flag", lit(false))
        .withColumn("inactive_date", functions.current_timestamp())

      addUpdatedColumnsInWorkTable.write.mode(SaveMode.Append)
        .option("columnOrder", "name")
        .saveAsTable(workTable)
    }
  }

  def addRecordsFromTargetWithActiveFlagFalseAndDiffIdIntoWorkTable(session: Session, targetTable: String, workTable: String, filterColumn: String, isFullLoadTrueForBatchOrProcess: Boolean) = {
    if (!isFullLoadTrueForBatchOrProcess) {
      //##### Adding records from target table with active_flag false into work table
      val targetToDfUpdateSql =
        s"""
      SELECT *
      FROM ${targetTable}_hist tgt
      WHERE EXISTS (
          SELECT 1
          FROM ${workTable} wk
          WHERE active_flag = true AND wk.${filterColumn} = tgt.${filterColumn} AND wk.tenant_code = tgt.tenant_code
      )
      AND tgt.active_flag = true
      AND NOT EXISTS (
          SELECT 1
          FROM ${workTable} wrk
          WHERE active_flag = true AND wrk.id = tgt.id
      )
    """

      val targetToDfUpdateDf = session.sql(targetToDfUpdateSql)

      val dropTargetColumnsForUpdatingDf = targetToDfUpdateDf.drop(Seq("active_flag", "inactive_date"))
      val addUpdatedColumnsInWorkTable = dropTargetColumnsForUpdatingDf
        .withColumn("active_flag", lit(false))
        .withColumn("inactive_date", functions.current_timestamp())

      addUpdatedColumnsInWorkTable.write.mode(SaveMode.Append)
        .option("columnOrder", "name")
        .saveAsTable(workTable)
    }
  }

  def addRecordsFromTargetWithActiveFlagFalseIntoWorkTableByTenantCode(session: Session, targetTable: String, workTable: String, filterColumn: String, isFullLoadTrueForBatchOrProcess: Boolean) = {
    if (!isFullLoadTrueForBatchOrProcess) {
      //##### Adding records from target table with active_flag false into work table
      val filterColumnList = filterColumn.split(",")
      val filterColumn1 = filterColumnList(0)
      val filterColumn2 = filterColumnList(1)
      val targetDataSql =
        s"""SELECT *
           |FROM ${targetTable}_hist t
           |WHERE EXISTS (
           |    SELECT 1
           |    FROM $workTable w
           |    WHERE w.$filterColumn1 = t.$filterColumn1
           |      AND w.$filterColumn2 = t.$filterColumn2
           |      AND w.active_flag = TRUE
           |)
           |AND t.active_flag = TRUE
           |""".stripMargin

      val targetToDfUpdateDf = session.sql(targetDataSql)
      val dropTargetColumnsForUpdatingDf = targetToDfUpdateDf.drop(Seq("active_flag", "inactive_date"))
      val addUpdatedColumnsInWorkTable = dropTargetColumnsForUpdatingDf
        .withColumn("active_flag", lit(false))
        .withColumn("inactive_date", functions.current_timestamp())

      addUpdatedColumnsInWorkTable.write.mode(SaveMode.Append)
        .option("columnOrder", "name")
        .saveAsTable(workTable)
    }
  }

  def addIdColumnAndCompareUdlDataForAggregates(session: Session, sequenceIdCurrentValue: Long, deltaDF: DataFrame, udlTable: String, orderByColumn: String, isFullLoadTrueForBatchOrProcess: Boolean): DataFrame = {
    // Define window specification
    val windowSpec = Window.orderBy(functions.col(orderByColumn))

    if (isFullLoadTrueForBatchOrProcess) {
      deltaDF.withColumn("id", functions.row_number().over(windowSpec) + sequenceIdCurrentValue)
    }
    else {
      val deltaDfWithTempId = deltaDF.withColumn("id_temp", functions.row_number().over(windowSpec) + sequenceIdCurrentValue)

      val recordsFromTargetTableToBeUpdatedDf = session.table(udlTable)
        .filter(col("code") in (deltaDfWithTempId.select(col("code"))))
        .select(col("code").as("target_code"), col("id").as("target_id"))

      val finalUpdatedDf = deltaDfWithTempId.join(recordsFromTargetTableToBeUpdatedDf, deltaDfWithTempId.col("code") === recordsFromTargetTableToBeUpdatedDf.col("target_code"), "left")
        .select(
          deltaDfWithTempId.col("*"),
          when(recordsFromTargetTableToBeUpdatedDf.col("target_id").is_null, deltaDfWithTempId.col("id_temp")).otherwise(recordsFromTargetTableToBeUpdatedDf.col("target_id")).as("id"))

      finalUpdatedDf.drop("id_temp")
    }
  }

  def setDataProcessStartTimeForAggregates(isFullLoadRunningForProcess: Boolean) = {
    if(isFullLoadRunningForProcess) "2000-01-01 00:00:00.000" else batchRunConfig.batchStartTime
  }

  def setDataProcessStartTimeForDailyBatchAggregates(isFullLoadRunningForProcess: Boolean) = {
    if (isFullLoadRunningForProcess) "2000-01-01 00:00:00.000" else batchRunConfig.dataProcessStartTime.substring(0, 10) + " 00:00:00.000"
  }

  def createDailySnapshotData(session: Session, udlTable: String, snapshotType: String, createdBatchRunId: Int, updatedBatchRunId: Any): Unit = {
    val tableName = udlTable.slice(4, udlTable.length)
    val mainTableDF = session.table(s"udl_batch_process.swap_${tableName}_hist")
    val snapshotTable = s"udl.${snapshotType}_${tableName}_snapshot"
    val snapshotWrkTable = s"udl_batch_process.wrk_${snapshotType}_${tableName}_snapshot"
    val sequenceIdCurrentValue: Long = session.sql("SELECT NVL(MAX(id),0) from %s".format(snapshotTable)).collect()(0).getLong(0)
    val windowSpec = Window.orderBy(functions.col("code"))

    // Get max snapshot date from snapshot table
    val maxSnapshotDateOpt = session.table(snapshotTable)
      .select(max(col("snapshot_date")))
      .collect()
      .head
      .get(0)

    // If table is empty, set maxSnapshotDate to today
    val lastSnapshotDate = if (maxSnapshotDateOpt == null) Instant.now().atZone(ZoneOffset.UTC).toLocalDate
    else maxSnapshotDateOpt.asInstanceOf[Date].toLocalDate

    val today = Instant.now().atZone(ZoneOffset.UTC).toLocalDate

    val dateSeq = if (lastSnapshotDate == today) {
      Seq(today)
    } else {
      Iterator.iterate(lastSnapshotDate.plusDays(1))(_.plusDays(1))
        .takeWhile(!_.isAfter(today))
        .toSeq
    }

    dateSeq.foreach { date =>
      mainTableDF
        .filter(col("active_flag")===lit(true) && col("code") =!= lit("N/A"))
        .withColumn("snapshot_date", lit(date.toString))
        .withColumn("snapshot_created_datetime", lit(current_timestamp()))
        .withColumn("snapshot_updated_datetime", lit(null))
        .withColumn("snapshot_created_by", lit(current_user()))
        .withColumn("snapshot_updated_by", lit(null))
        .withColumn("snapshot_created_batch_run_id", lit(createdBatchRunId))
        .withColumn("snapshot_updated_batch_run_id", lit(updatedBatchRunId))
        .withColumn("code", concat(col("code"), lit("-") ,col("snapshot_date")))
        .write.mode("append")
        .option("columnOrder", "name")
        .saveAsTable(snapshotWrkTable)
    }

      session.table(snapshotWrkTable)
      .withColumn("id", functions.row_number().over(windowSpec) + sequenceIdCurrentValue)
      .write.mode("overwrite")
      .saveAsTable(snapshotWrkTable)
    PublishToSwappedTargetTable.initiateWrkToSwappedTargetTableProcess(session, s"${snapshotTable}", "scdType1Snapshot",  isFullLoadTrueForBatchOrProcess = false)
  }


  def createMonthlySnapshot(session: Session, monthlySnapshotTable: String, dailySnapshotTable: String, createdBatchRunId: Int, updatedBatchRunId: Any): Unit = {
    val tableName = monthlySnapshotTable.slice(4, monthlySnapshotTable.length)
    val monthlySnapshotWrkTable = s"udl_batch_process.wrk_${tableName}"
    val sequenceIdCurrentValue: Long = session.sql("SELECT NVL(MAX(id),0) from %s".format(monthlySnapshotTable)).collect()(0).getLong(0)
    val windowSpec = Window.orderBy(functions.col("code"))

    // Determine the first day of the previous month
    val today = Instant.now().atZone(ZoneOffset.UTC).toLocalDate
    val firstDayPrevMonth = today.minusMonths(1).withDayOfMonth(1)

    // Get the max snapshot date from the monthly table
    val maxMonthlySnapshotDateOpt = session.table(monthlySnapshotTable)
      .select(max(col("snapshot_date")))
      .collect()
      .head
      .get(0)

    // If monthly table is empty, start from the previous month; else, continue from the next month
    val startMonth = if (maxMonthlySnapshotDateOpt == null) firstDayPrevMonth else {
      maxMonthlySnapshotDateOpt.asInstanceOf[Date].toLocalDate.plusMonths(1)
    }

    // Generate the list of missing months until the previous month
    val dateSeq = Iterator.iterate(startMonth)(_.plusMonths(1))
      .takeWhile(!_.isAfter(firstDayPrevMonth))

    dateSeq.foreach { monthDate =>
      // Get the max daily snapshot date for the given month
      val maxDailySnapshotDateOpt = session.table(dailySnapshotTable)
        .filter(month(col("snapshot_date")) === lit(monthDate.getMonthValue) &&
          year(col("snapshot_date")) === lit(monthDate.getYear))
        .select(max(col("snapshot_date")))
        .collect()
        .head
        .get(0)

      if (maxDailySnapshotDateOpt != null) {
        val maxDailySnapshotDate = maxDailySnapshotDateOpt.asInstanceOf[Date].toLocalDate

        session.table(dailySnapshotTable)
          .filter(col("snapshot_date") === lit(maxDailySnapshotDate.toString))
          .withColumn("snapshot_date", lit(monthDate.toString))
          .withColumn("snapshot_created_datetime", lit(current_timestamp()))
          .withColumn("snapshot_updated_datetime", lit(null))
          .withColumn("snapshot_created_by", lit(current_user()))
          .withColumn("snapshot_updated_by", lit(null))
          .withColumn("snapshot_created_batch_run_id", lit(createdBatchRunId))
          .withColumn("snapshot_updated_batch_run_id", lit(updatedBatchRunId))
          .withColumn("code", concat(col("tenant_code"), lit("-") ,col("snapshot_date")))
          .write.mode("append")
          .option("columnOrder", "name")
          .saveAsTable(monthlySnapshotWrkTable)

      } else {
        println(s"No daily snapshot data found for ${monthDate.getMonthValue}-${monthDate.getYear}")
      }
    }

    session.table(monthlySnapshotWrkTable)
      .withColumn("id", functions.row_number().over(windowSpec) + sequenceIdCurrentValue)
      .write.mode("overwrite")
      .saveAsTable(monthlySnapshotWrkTable)

    PublishToSwappedTargetTable.initiateWrkToSwappedTargetTableProcess(
      session, monthlySnapshotTable, "scdType1Snapshot", isFullLoadTrueForBatchOrProcess = false
    )
  }

  def setDataIntoAggregatesSnapshotSwap(session: Session, udlTable: String, aggregateType: String): Unit = {
    val tableName = udlTable.slice(4, udlTable.length)
    val deltaDf = session.table(s"udl_batch_process.swap_${tableName}");
    var selectedDataFromDeltaWrk: DataFrame = null;

    if(aggregateType == "daily"){
      val snappedMaxDate = session.sql(s"SELECT NVL(max(reporting_date), '2000-01-01') FROM ${udlTable}_snapshot").collect()(0).getDate(0)
      selectedDataFromDeltaWrk = deltaDf.filter(col("reporting_date") < current_date() && col("reporting_date") >= snappedMaxDate)
        .select(col("*"))

    }else if(aggregateType == "weekly"){
      val snappedMaxDate = session.sql(s"SELECT NVL(max(reporting_week), '2000-01-01') FROM ${udlTable}_snapshot").collect()(0).getDate(0)
      selectedDataFromDeltaWrk = deltaDf.filter(col("reporting_week") < date_trunc("week", current_date()) && col("reporting_week") >= lit(snappedMaxDate))
        .select(col("*"))

    }else{
      val snappedMaxDate = session.sql(s"SELECT NVL(max(reporting_month), '2000-01-01') FROM ${udlTable}_snapshot").collect()(0).getDate(0)
      selectedDataFromDeltaWrk = deltaDf.filter(col("reporting_month") < date_trunc("month", current_date()) && col("reporting_month") >= lit(snappedMaxDate))
        .select(col("*"))

    }
    selectedDataFromDeltaWrk.write.mode(SaveMode.Overwrite).saveAsTable(s"udl_batch_process.wrk_${tableName}_snapshot")
    PublishToSwappedTargetTable.initiateWrkToSwappedTargetTableProcess(session, s"${udlTable}_snapshot", "core",  isFullLoadTrueForBatchOrProcess = false)
  }

  def dataBackfillForAggregates(session: Session, udlTable: String, aggregateType: String): Unit = {
    val tableName = udlTable.slice(4, udlTable.length)
    val remainingDataDf = session.sql(s"SELECT * FROM udl.${tableName}_snapshot AS snapped " +
      s"WHERE NOT EXISTS (SELECT 1 FROM udl_batch_process.wrk_${tableName} AS wrk WHERE snapped.code = wrk.code) ")

    remainingDataDf.write.mode(SaveMode.Overwrite)
      .saveAsTable(s"udl_batch_process.data_from_snapshot_${tableName}")

    session.table(s"udl_batch_process.data_from_snapshot_${tableName}").write.mode(SaveMode.Append).option("columnOrder", "name")
      .saveAsTable("udl_batch_process.wrk_daily_user_adoption")
  }

  def getTenantDetailsData(session: Session,  deltaDF: DataFrame): DataFrame = {
    val tenantDetailsDf = session.table("udl_batch_process.swap_tenant_details_hist")
      .filter(col("active_flag") === true)
      .groupBy("code").any_value(col("is_recognition_enabled"),col("is_employee_listening_enabled"))
      .select(
        col("code").as("tenant_details_code"),
        col("ANY_VALUE(IS_RECOGNITION_ENABLED)").as("is_recognition_enabled"),
        col("ANY_VALUE(IS_EMPLOYEE_LISTENING_ENABLED)").as("is_employee_listening_enabled")
      );
    tenantDetailsDf
  }

  def getTenantsAccountCodeMappingData(session: Session): DataFrame = {
    val refTenantDetailsDf = session.sql("""SELECT account_code, ANY_VALUE(tenant_code) AS tenant_code FROM
                                           |(
                                           |SELECT
                                           |AFTER_SNAPSHOT:org_id ::STRING AS tenant_code,
                                           |AFTER_SNAPSHOT:Simpplr__UUID__c::STRING AS account_code
                                           |FROM
                                           |core_raw_latest.core_simpplr__app_config_latest
                                           |UNION ALL
                                           |SELECT
                                           |domain_payload: account_id :: STRING AS tenant_code,
                                           |domain_payload: account_id :: STRING AS account_code
                                           |FROM zeus_raw_latest.account_zeus_account_details_latest
                                           |)
                                           |GROUP BY account_code""".stripMargin)
    refTenantDetailsDf
  }

  def getSalesforcefTenantsAccountCodeMappingData(session: Session): DataFrame = {
    val refTenantDetailsDf = session.sql("""SELECT account_code, ANY_VALUE(tenant_code) AS sf_tenant_code FROM
                                           |(
                                           |SELECT
                                           |AFTER_SNAPSHOT:org_id ::STRING AS tenant_code,
                                           |AFTER_SNAPSHOT:Simpplr__UUID__c::STRING AS account_code
                                           |FROM
                                           |core_raw_latest.core_simpplr__app_config_latest
                                           |)
                                           |GROUP BY account_code""".stripMargin)
    refTenantDetailsDf
  }

  def prepareRawTableIdsToArchive(inputDeltaDf: DataFrame, sourceEntityName: String, stgArchiveTable: String, batchRunId: Int): Unit = {
    val idsToArchiveDf = inputDeltaDf.select(col("staging_id").as("archive_id")).union(inputDeltaDf.select(col("header_id").as("archive_id"))).distinct()
    idsToArchiveDf
      .withColumn("source_entity_name", lit(sourceEntityName))
      .withColumn("batch_run_id", lit(batchRunId)).write.mode(SaveMode.Append)
      .saveAsTable(stgArchiveTable)
  }

  def insertRawTableIdsToArchive(inputDeltaDf: DataFrame, sourceEntityName: String, stgArchiveTable: String, batchRunId: Int): Unit = {
    val idsToArchiveDf = inputDeltaDf.select(col("staging_id").as("archive_id"))
    idsToArchiveDf
      .withColumn("source_entity_name", lit(sourceEntityName))
      .withColumn("batch_run_id", lit(batchRunId))
      .write.mode(SaveMode.Append).saveAsTable(stgArchiveTable)
  }

  def insertRawTableIdsWithHeaderIdsToArchive(inputDeltaDf: DataFrame, sourceEntityName: String, stgArchiveTable: String, batchRunId: Int): Unit = {
    val idsToArchiveDf = inputDeltaDf.select(col("staging_id").as("archive_id")).union(inputDeltaDf.select(col("header_id").as("archive_id"))).distinct()
    idsToArchiveDf
      .withColumn("source_entity_name", lit(sourceEntityName))
      .withColumn("batch_run_id", lit(batchRunId))
      .write.mode(SaveMode.Append).saveAsTable(stgArchiveTable)
  }

  def updateRawLatestTable(sourceSchema: String, sourceEntity: String, sourceColumn: String, dataProcessEndTime: String): Unit = {
    session.sql(s"SELECT MAX(id) AS maxId, ${sourceColumn} AS unique_id " +
      s"FROM ${sourceSchema}.${sourceEntity} " +
      s"WHERE created_datetime <= '${dataProcessEndTime}' AND after_snapshot::STRING IS NOT NULL " +
      s"GROUP BY ${sourceColumn} ")
    .write.mode(SaveMode.Overwrite).saveAsTable(s"udl_batch_process.${sourceEntity}_delta_max_ids")

    session.sql(s"SELECT d.* FROM ${sourceSchema}.${sourceEntity} d " +
      s"WHERE EXISTS (SELECT 1 FROM udl_batch_process.${sourceEntity}_delta_max_ids AS m WHERE m.maxId = d.id) " +
      "UNION ALL " +
      s"SELECT l.* FROM ${sourceSchema}_latest.${sourceEntity}_latest l " +
      s"WHERE NOT EXISTS (SELECT 1 FROM udl_batch_process.${sourceEntity}_delta_max_ids AS m WHERE m.unique_id = l.${sourceColumn} )")
    .write.mode(SaveMode.Overwrite).saveAsTable(s"${sourceSchema}_latest.${sourceEntity}_latest")
  }

  def updateTenantRawLatestTable(sourceSchema: String, sourceEntity: String, sourceColumn: String, sourceTenantColumn: String, dataProcessEndTime: String): Unit = {
    session.sql(s"SELECT MAX(id) AS maxId, ${sourceColumn} AS unique_id,${sourceTenantColumn} AS tenant_code " +
        s"FROM ${sourceSchema}.${sourceEntity} " +
        s"WHERE created_datetime <= '${dataProcessEndTime}' AND after_snapshot::STRING IS NOT NULL " +
        s"GROUP BY ${sourceColumn}, ${sourceTenantColumn}")
      .write.mode(SaveMode.Overwrite).saveAsTable(s"udl_batch_process.${sourceEntity}_delta_max_ids")

    session.sql(s"SELECT d.* FROM ${sourceSchema}.${sourceEntity} d " +
        s"WHERE EXISTS (SELECT 1 FROM udl_batch_process.${sourceEntity}_delta_max_ids AS m WHERE m.maxId = d.id) " +
        "UNION ALL " +
        s"SELECT l.* FROM ${sourceSchema}_latest.${sourceEntity}_latest l " +
        s"WHERE NOT EXISTS (SELECT 1 FROM udl_batch_process.${sourceEntity}_delta_max_ids AS m " +
        s"WHERE m.unique_id = l.${sourceColumn} AND m.tenant_code = l.${sourceTenantColumn})")
      .write.mode(SaveMode.Overwrite).saveAsTable(s"${sourceSchema}_latest.${sourceEntity}_latest")
  }

  def updateViewLatestTable(sourceSchema: String, sourceEntity: String, sourceColumn: String, dataProcessEndTime: String): Unit = {
    session.sql(s"SELECT MAX(id) AS maxId, ${sourceColumn} AS unique_id " +
      s"FROM ${sourceSchema}.vw_${sourceEntity} " +
      s"WHERE created_datetime <= '${dataProcessEndTime}' AND ${sourceColumn} IS NOT NULL " +
      s"GROUP BY ${sourceColumn} ")
      .write.mode(SaveMode.Overwrite).saveAsTable(s"udl_batch_process.${sourceEntity}_delta_max_ids")

    session.sql(s"SELECT d.* FROM ${sourceSchema}.vw_${sourceEntity} d " +
      s"WHERE EXISTS (SELECT 1 FROM udl_batch_process.${sourceEntity}_delta_max_ids AS m WHERE m.maxId = d.id) " +
      "UNION ALL " +
      s"SELECT l.* FROM ${sourceSchema}_latest.${sourceEntity}_latest l " +
      s"WHERE NOT EXISTS (SELECT 1 FROM udl_batch_process.${sourceEntity}_delta_max_ids AS m WHERE m.unique_id = l.${sourceColumn} )")
      .write.mode(SaveMode.Overwrite).saveAsTable(s"${sourceSchema}_latest.${sourceEntity}_latest")
  }

  def updateTenantViewLatestTable(sourceSchema: String, sourceEntity: String, sourceColumn: String, sourceTenantColumn: String, dataProcessEndTime: String): Unit = {
    session.sql(s"SELECT MAX(id) AS maxId, ${sourceColumn} AS unique_id, ${sourceTenantColumn} AS tenant_code " +
        s"FROM ${sourceSchema}.vw_${sourceEntity} " +
        s"WHERE created_datetime <= '${dataProcessEndTime}' AND ${sourceColumn} IS NOT NULL " +
        s"GROUP BY ${sourceColumn},${sourceTenantColumn} ")
      .write.mode(SaveMode.Overwrite).saveAsTable(s"udl_batch_process.${sourceEntity}_delta_max_ids")

    session.sql(s"SELECT d.* FROM ${sourceSchema}.vw_${sourceEntity} d " +
        s"WHERE EXISTS (SELECT 1 FROM udl_batch_process.${sourceEntity}_delta_max_ids AS m WHERE m.maxId = d.id) " +
        "UNION ALL " +
        s"SELECT l.* FROM ${sourceSchema}_latest.${sourceEntity}_latest l " +
        s"WHERE NOT EXISTS (SELECT 1 FROM udl_batch_process.${sourceEntity}_delta_max_ids AS m " +
        s"WHERE m.unique_id = l.${sourceColumn} AND m.tenant_code = l.${sourceTenantColumn})")
      .write.mode(SaveMode.Overwrite).saveAsTable(s"${sourceSchema}_latest.${sourceEntity}_latest")
  }

  def updateViewAggregatedLatestTable(sourceSchema: String, sourceEntity: String, sourceColumnFirst: String, sourceColumnSecond: String, dataProcessEndTime: String): Unit = {
    session.sql(s"SELECT MAX(id) AS maxId, ${sourceColumnFirst} AS unique_id, ${sourceColumnSecond} as aggregate_type " +
      s"FROM ${sourceSchema}.vw_${sourceEntity} " +
      s"WHERE created_datetime <= '${dataProcessEndTime}' AND ${sourceColumnFirst} IS NOT NULL OR ${sourceColumnSecond} IS NOT NULL " +
      s"GROUP BY ${sourceColumnFirst}, ${sourceColumnSecond} ")
      .write.mode(SaveMode.Overwrite).saveAsTable(s"udl_batch_process.${sourceEntity}_delta_max_ids")

    session.sql(s"SELECT d.* FROM ${sourceSchema}.vw_${sourceEntity} d " +
      s"WHERE EXISTS (SELECT 1 FROM udl_batch_process.${sourceEntity}_delta_max_ids AS m WHERE m.maxId = d.id) " +
      "UNION ALL " +
      s"SELECT l.* FROM ${sourceSchema}_latest.${sourceEntity}_latest l " +
      s"WHERE NOT EXISTS (SELECT 1 FROM udl_batch_process.${sourceEntity}_delta_max_ids AS m WHERE m.unique_id = l.${sourceColumnFirst} and m.aggregate_type = l.${sourceColumnSecond})")
      .write.mode(SaveMode.Overwrite).saveAsTable(s"${sourceSchema}_latest.${sourceEntity}_latest")

  }

  def updateTenantViewAggregatedLatestTable(sourceSchema: String, sourceEntity: String, sourceColumnFirst: String, sourceTenantColumn: String, sourceColumnSecond: String, dataProcessEndTime: String): Unit = {
    session.sql(s"SELECT MAX(id) AS maxId, ${sourceColumnFirst} AS unique_id, ${sourceColumnSecond} as aggregate_type,  ${sourceTenantColumn} AS tenant_code " +
        s"FROM ${sourceSchema}.vw_${sourceEntity} " +
        s"WHERE created_datetime <= '${dataProcessEndTime}' AND ${sourceColumnFirst} IS NOT NULL OR ${sourceColumnSecond} IS NOT NULL " +
        s"GROUP BY ${sourceColumnFirst}, ${sourceColumnSecond}, ${sourceTenantColumn} ")
      .write.mode(SaveMode.Overwrite).saveAsTable(s"udl_batch_process.${sourceEntity}_delta_max_ids")

    session.sql(s"SELECT d.* FROM ${sourceSchema}.vw_${sourceEntity} d " +
        s"WHERE EXISTS (SELECT 1 FROM udl_batch_process.${sourceEntity}_delta_max_ids AS m WHERE m.maxId = d.id) " +
        "UNION ALL " +
        s"SELECT l.* FROM ${sourceSchema}_latest.${sourceEntity}_latest l " +
        s"WHERE NOT EXISTS (SELECT 1 FROM udl_batch_process.${sourceEntity}_delta_max_ids AS m WHERE m.unique_id = l.${sourceColumnFirst} AND m.aggregate_type = l.${sourceColumnSecond} AND m.tenant_code = l.${sourceTenantColumn})")
      .write.mode(SaveMode.Overwrite).saveAsTable(s"${sourceSchema}_latest.${sourceEntity}_latest")
  }

  def updateViewGroupedLatestTable(sourceSchema: String, sourceEntity: String, sourceColumnFirst: String, sourceColumnSecond: String, sourceColumnThird: String, dataProcessEndTime: String): Unit = {
    session.sql(s"SELECT MAX(id) AS maxId, ${sourceColumnFirst} AS account_id, ${sourceColumnSecond} as aggregate,${sourceColumnThird} as namespace " +
        s"FROM ${sourceSchema}.vw_${sourceEntity} " +
        s"WHERE created_datetime <= '${dataProcessEndTime}' AND ${sourceColumnFirst} IS NOT NULL OR (${sourceColumnSecond} IS NOT NULL OR ${sourceColumnThird} IS NOT NULL) " +
        s"GROUP BY ${sourceColumnFirst}, ${sourceColumnSecond},${sourceColumnThird} ")
      .write.mode(SaveMode.Overwrite).saveAsTable(s"udl_batch_process.${sourceEntity}_delta_max_ids")

    session.sql(s"SELECT d.* FROM ${sourceSchema}.vw_${sourceEntity} d " +
        s"WHERE EXISTS (SELECT 1 FROM udl_batch_process.${sourceEntity}_delta_max_ids AS m WHERE m.maxId = d.id) " +
        "UNION ALL " +
        s"SELECT l.* FROM ${sourceSchema}_latest.${sourceEntity}_latest l " +
        s"WHERE NOT EXISTS (SELECT 1 FROM udl_batch_process.${sourceEntity}_delta_max_ids AS m WHERE m.account_id = l.${sourceColumnFirst} and m.aggregate = l.${sourceColumnSecond}  and m.namespace = l.${sourceColumnThird})")
      .write.mode(SaveMode.Overwrite).saveAsTable(s"${sourceSchema}_latest.${sourceEntity}_latest")

  }

  def eliminateDuplicatesOnCodeFromWrkTable(session: Session, wrkTable: String, scdType: String, sortingColumn: String): Unit = {
    val windowSpecZeus = Window.partitionBy(col("tenant_code"),col("code")).orderBy(col("is_deleted").desc, col(sortingColumn).desc)
    if(scdType == "scd1"){
      val rankedDfZeus = session.table(wrkTable)
        .withColumn("rank", row_number().over(windowSpecZeus))

      // Select only the latest record in each partition
      val wrkDeltaWithLatestDataDfZeus = rankedDfZeus.where(col("rank") === 1).drop("rank")

      wrkDeltaWithLatestDataDfZeus.write.mode(SaveMode.Overwrite).saveAsTable(wrkTable)
    }else{
      val rankedDfZeus = session.table(wrkTable)
        .filter(col("active_flag") === lit(true))
        .withColumn("rank", row_number().over(windowSpecZeus))

      // Select only the latest record in each partition
      val wrkDeltaWithLatestDataDfZeus = rankedDfZeus.where(col("rank") === 1).drop("rank")

      val joinedRankedDataDf = session.table(wrkTable)
        .filter(col("active_flag") === lit(false))
        .union(wrkDeltaWithLatestDataDfZeus)
      joinedRankedDataDf.write.mode(SaveMode.Overwrite).saveAsTable(wrkTable)
    }
  }

  def eliminateDuplicatesOnIdFromWrkTable(session: Session, wrkTable: String, sortingColumn: String): Unit = {
    val windowSpecZeus = Window.partitionBy(col("id")).orderBy(col("is_deleted").desc, col(sortingColumn).desc)
    val rankedDfZeus = session.table(wrkTable)
      .withColumn("rank", row_number().over(windowSpecZeus))

    // Select only the latest record in each partition
    val wrkDeltaWithLatestDataDfZeus = rankedDfZeus.where(col("rank") === 1).drop("rank")

    wrkDeltaWithLatestDataDfZeus.write.mode(SaveMode.Overwrite).saveAsTable(wrkTable)
  }

  def fetchMigratedTenantCodes(session: Session): DataFrame = {
    val migrationBatchRunDf = session.sql("""SELECT
                                            |    tenant_id AS tenant_code,
                                            |    stage_to_raw_end_time AS migration_completed_datetime
                                            |FROM
                                            |    migration.historical_migration_batch_run,
                                            |    WHERE LOWER(stage_to_raw_status) = 'completed'
                                            |    AND stage_to_raw_end_time IS NOT NULL """.stripMargin)

    migrationBatchRunDf.write.mode(SaveMode.Overwrite).saveAsTable("udl_batch_process.migrated_tenant_codes")

    session.table("udl_batch_process.migrated_tenant_codes")
  }

  def assignUpdateInfo(session: Session, stagingTable: String, target: String, isFullLoadRunningForProcess: Boolean): DataFrame = {
    val stagingDf = session.table(stagingTable)
    if (isFullLoadRunningForProcess) {
      stagingDf
        .withColumn("created_by", lit(current_user()))
        .withColumn("created_datetime", lit(current_timestamp()))
        .withColumn("updated_by", lit(null))
        .withColumn("updated_datetime", lit(current_timestamp()))
        .withColumn("created_batch_run_id", lit(batchRunConfig.batchRunId))
        .withColumn("updated_batch_run_id", lit(null))
    }
    else {
      val recordsFromTargetTableToBeUpdatedDf = session.sql(s"SELECT " +
        s"d.code AS target_code, " +
        s"d.created_datetime AS target_created_datetime, " +
        s"d.created_by AS target_created_by, " +
        s"d.created_batch_run_id AS target_created_batch_run_id " +
        s"FROM ${target} d " +
        s"WHERE EXISTS (SELECT 1 FROM ${stagingTable} AS m WHERE m.code = d.code) ")

      val finalUpdatedDf = stagingDf.join(recordsFromTargetTableToBeUpdatedDf, stagingDf.col("code") === recordsFromTargetTableToBeUpdatedDf.col("target_code"), "left")
        .select(
          stagingDf.col("*"),
          when(recordsFromTargetTableToBeUpdatedDf.col("target_created_datetime").is_null, lit(current_timestamp())).otherwise(recordsFromTargetTableToBeUpdatedDf.col("target_created_datetime")).as("created_datetime"),
          when(recordsFromTargetTableToBeUpdatedDf.col("target_created_by").is_null, lit(current_user())).otherwise(recordsFromTargetTableToBeUpdatedDf.col("target_created_by")).as("created_by"),
          when(recordsFromTargetTableToBeUpdatedDf.col("target_created_batch_run_id").is_null, lit(batchRunConfig.batchRunId)).otherwise(recordsFromTargetTableToBeUpdatedDf.col("target_created_batch_run_id")).as("created_batch_run_id"),
          when(recordsFromTargetTableToBeUpdatedDf.col("target_code").is_null, lit(null)).otherwise(lit(current_timestamp())).as("updated_datetime"),
          when(recordsFromTargetTableToBeUpdatedDf.col("target_code").is_null, lit(null)).otherwise(lit(current_user())).as("updated_by"),
          when(recordsFromTargetTableToBeUpdatedDf.col("target_code").is_null, lit(null)).otherwise(lit(batchRunConfig.batchRunId)).as("updated_batch_run_id")
        )

      finalUpdatedDf
    }
  }
}