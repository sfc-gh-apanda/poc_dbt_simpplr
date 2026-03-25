package com.simpplr.configdata


import com.snowflake.snowpark._

object PublishToSwappedTargetTable {

  def initiateWrkToSwappedTargetTableProcess(session: Session, targetTable: String, platformType:String , isFullLoadTrueForBatchOrProcess: Boolean): Unit = {

    val tableInfo = session.sql(s" SELECT " +
    "DISTINCT lower(uem.target_table) as target_table, " +
    "lower(uem.work_table) as wrk_table, " +
    "lower(coalesce(history_table, target_table)) as hist_table," +
    "listagg( DISTINCT isc.column_name, ', ') " +
    "WITHIN GROUP (ORDER BY isc.column_name) " +
    "OVER (PARTITION BY uem.target_table) as column_name " +
    "FROM config.udl_entity_mapping AS uem " +
    "INNER JOIN information_schema.columns AS isc " +
    "ON lower(uem.target_table) = lower(isc.table_schema||'.'||isc.table_name) " +
    "WHERE uem.work_table is NOT NULL AND "+
    "uem.active_flag = TRUE AND " +
    "lower(isc.table_schema) = 'udl' AND " +
    s"target_table = lower('${targetTable}') LIMIT 1 "
    )

    // Collect the results into a DataFrame
    val tableInfoRaw = tableInfo.collect()

    // Access the table values
    val workTable = tableInfoRaw(0).getString(1)
    val historyTable:String = tableInfoRaw(0).getString(2)
    val columnNames = tableInfoRaw(0).getString(3)

//    println(workTable)
//    println(historyTable)
//    println(columnNames)

    val targetTableName = historyTable.slice(4, historyTable.length)
    println(targetTableName)
    val shared_transformations_target_table = List("udl.award_instance_hist", "udl.campaign_hist", "udl.campaign_assets_hist", "udl.user_company_values", "udl.company_values_hist")
    val createSwapTable = session.sql(s"CREATE OR REPLACE TABLE udl_batch_process.SWAP_${targetTableName} CLONE ${historyTable} ")
    createSwapTable.show()
    if(isFullLoadTrueForBatchOrProcess){
      if (platformType == "core"){
        if(shared_transformations_target_table.contains(historyTable) ){
           val deleteRecordFullLoadShared = session.sql(s"DELETE FROM udl_batch_process.SWAP_${targetTableName} AS tgt WHERE tgt.id != -1")
        deleteRecordFullLoadShared.show()
        } else {
          val deleteRecordFullLoadCore = session.sql(s"DELETE FROM udl_batch_process.SWAP_${targetTableName} AS tgt WHERE tgt.id != -1 ")
          deleteRecordFullLoadCore.show()
        }
      } else if(platformType == "zeus") {
        val deleteRecordFullLoadZeus = session.sql(s"DELETE FROM udl_batch_process.SWAP_${targetTableName} AS tgt WHERE tgt.id != -1 AND data_source_code = 'DS002' ")
        deleteRecordFullLoadZeus.show()
      }
      else {
        val deleteRecordAggregate = session.sql(s"DELETE FROM udl_batch_process.SWAP_${targetTableName} AS tgt WHERE tgt.id != -1")
        deleteRecordAggregate.show()
      }
    }
    else if(platformType == "scdType1Snapshot") {
      val deleteRecordUsingWrkTable = session.sql(s"DELETE FROM udl_batch_process.SWAP_${targetTableName} AS tgt WHERE EXISTS (SELECT 1 FROM ${workTable} AS wrk WHERE tgt.snapshot_date = wrk.snapshot_date) ")
      deleteRecordUsingWrkTable.show()
    }
    else if (targetTable == "udl.tenant_information") {
      val deleteRecordUsingWrkTable = session.sql(s"DELETE FROM udl_batch_process.SWAP_${targetTableName} AS tgt WHERE EXISTS (SELECT 1 FROM ${workTable} AS wrk WHERE tgt.id = wrk.id) ")
      deleteRecordUsingWrkTable.show()
    }
    else if (platformType != "monthly_snapshots" && platformType != "daily_snapshots") {
      val deleteRecordUsingWrkTable = session.sql(s"DELETE FROM udl_batch_process.SWAP_${targetTableName} AS tgt WHERE EXISTS (SELECT 1 FROM ${workTable} AS wrk WHERE tgt.id = wrk.id AND tgt.tenant_code = wrk.tenant_code) ")
      deleteRecordUsingWrkTable.show()
    }

    val insertWrkToSwapTable = session.sql(s"INSERT INTO  udl_batch_process.SWAP_${targetTableName}(${columnNames}) SELECT ${columnNames} FROM ${workTable} WHERE code IS NOT NULL ")
    insertWrkToSwapTable.show()
  }

  def initiateWrkToSwappedTargetTableProcessOnCode(session: Session, targetTable: String, platformType: String, isFullLoadTrueForBatchOrProcess: Boolean): Unit = {
    val tableInfo = session.sql(s" SELECT " +
      "DISTINCT lower(uem.target_table) as target_table, " +
      "lower(uem.work_table) as wrk_table, " +
      "lower(coalesce(history_table, target_table)) as hist_table," +
      "listagg( DISTINCT isc.column_name, ', ') " +
      "WITHIN GROUP (ORDER BY isc.column_name) " +
      "OVER (PARTITION BY uem.target_table) as column_name " +
      "FROM config.udl_entity_mapping AS uem " +
      "INNER JOIN information_schema.columns AS isc " +
      "ON lower(uem.target_table) = lower(isc.table_schema||'.'||isc.table_name) " +
      "WHERE uem.work_table is NOT NULL AND " +
      "uem.active_flag = TRUE AND " +
      "lower(isc.table_schema) = 'udl' AND " +
      s"target_table = lower('${targetTable}') LIMIT 1 "
    )

    // Collect the results into a DataFrame
    val tableInfoRaw = tableInfo.collect()

    // Access the table values
    val workTable = tableInfoRaw(0).getString(1)
    val historyTable: String = tableInfoRaw(0).getString(2)
    val columnNames = tableInfoRaw(0).getString(3)

    val targetTableName = historyTable.slice(4, historyTable.length)
    println(targetTableName)
    val shared_transformations_target_table = List("udl.award_instance_hist", "udl.campaign_hist", "udl.campaign_assets_hist", "udl.user_company_values", "udl.company_values_hist")
    val createSwapTable = session.sql(s"CREATE OR REPLACE TABLE udl_batch_process.SWAP_${targetTableName} CLONE ${historyTable} ")
    createSwapTable.show()
    if (isFullLoadTrueForBatchOrProcess) {
      if (platformType == "core") {
        if (shared_transformations_target_table.contains(historyTable)) {
          val deleteRecordFullLoadShared = session.sql(s"DELETE FROM udl_batch_process.SWAP_${targetTableName} AS tgt WHERE tgt.code != 'N/A' ")
          deleteRecordFullLoadShared.show()
        } else {
          val deleteRecordFullLoadCore = session.sql(s"DELETE FROM udl_batch_process.SWAP_${targetTableName} AS tgt WHERE tgt.code != 'N/A'  ")
          deleteRecordFullLoadCore.show()
        }
      } else if (platformType == "zeus") {
        val deleteRecordFullLoadZeus = session.sql(s"DELETE FROM udl_batch_process.SWAP_${targetTableName} AS tgt WHERE tgt.code != 'N/A'  AND data_source_code = 'DS002' ")
        deleteRecordFullLoadZeus.show()
      }
      else {
        val deleteRecordAggregate = session.sql(s"DELETE FROM udl_batch_process.SWAP_${targetTableName} AS tgt WHERE tgt.code != 'N/A' ")
        deleteRecordAggregate.show()
      }
    }
    else if (platformType == "scdType1Snapshot") {
      val deleteRecordUsingWrkTable = session.sql(s"DELETE FROM udl_batch_process.SWAP_${targetTableName} AS tgt WHERE EXISTS (SELECT 1 FROM ${workTable} AS wrk WHERE tgt.snapshot_date = wrk.snapshot_date) ")
      deleteRecordUsingWrkTable.show()
    }
    else if (platformType != "monthly_snapshots" && platformType != "daily_snapshots") {
      val deleteRecordUsingWrkTable = session.sql(s"DELETE FROM udl_batch_process.SWAP_${targetTableName} AS tgt WHERE EXISTS (SELECT 1 FROM ${workTable} AS wrk WHERE tgt.code = wrk.code AND tgt.tenant_code = wrk.tenant_code ) ")
      deleteRecordUsingWrkTable.show()
    }

    val insertWrkToSwapTable = session.sql(s"INSERT INTO  udl_batch_process.SWAP_${targetTableName}(${columnNames}) SELECT ${columnNames} FROM ${workTable} WHERE code IS NOT NULL ")
    insertWrkToSwapTable.show()
  }

  def initiateWrkToSwappedProcessForMeasuresTables(session: Session, targetTable: String){
    val targetTableName = targetTable.slice(4, targetTable.length)
    val createSwapTable = session.sql(s"CREATE OR REPLACE TABLE udl_batch_process.SWAP_${targetTableName} CLONE udl_batch_process.wrk_${targetTableName} ")
    createSwapTable.show()
  }
}