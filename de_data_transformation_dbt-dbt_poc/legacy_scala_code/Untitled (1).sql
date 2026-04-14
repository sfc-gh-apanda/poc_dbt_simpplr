CREATE OR REPLACE PROCEDURE udl_batch_process.prc_udl_publish_to_target_without_swap(params_json varchar)
	RETURNS VARIANT 
	LANGUAGE JAVASCRIPT
	STRICT 
	EXECUTE AS OWNER
	AS
	$$

	class LogMessage {
		constructor(t_id, severity, code, msg) {
			this.process_name = "udl_data_load";
			this.sub_process_name = "prc_udl_manage_transient_objects";
			this.transaction_identifier = t_id;
			this.severity = severity;
			this.log_code = code;
			this.log_description = msg;
		}
	}

	class Error {
		constructor(code, msg) {
			this.err_code = code;
			this.err_msg = msg;
			this.sub_process_parameter_value = PARAMS_JSON;
		}
	}

	try {
		try {
			var paramValues = JSON.parse(PARAMS_JSON);
		} catch (err) {
			throw new Error("E1000", "Invalid JSON passed in parameter");
		}

		if (paramValues.batch_run_id) {
			var batchRunId = paramValues.batch_run_id;
			var fullLoad = ''
			var coreOrZeus = ''
			var rsCheckFullLoad = snowflake.createStatement({
				sqlText: "SELECT full_load, CORE_OR_ZEUS, process_running_in_full_load FROM execution_run_stats.batch_run WHERE batch_run_id = :1 AND active_flag = TRUE",
				binds: [batchRunId]
			}).execute();

			if (rsCheckFullLoad.next()) {
				fullLoad =  rsCheckFullLoad.getColumnValue(1);
				coreOrZeus = rsCheckFullLoad.getColumnValue(2);
				process_running_in_full_load = rsCheckFullLoad.getColumnValue(3);
			}

			var rsGetTables = snowflake.createStatement({
				sqlText: "SELECT DISTINCT lower(uem.target_table), lower(uem.work_table), lower(coalesce(history_table, target_table)), listagg( DISTINCT isc.column_name, ', ') WITHIN GROUP (ORDER BY isc.column_name) OVER (PARTITION BY uem.target_table)FROM config.udl_entity_mapping AS uem INNER JOIN information_schema.columns AS isc ON lower(uem.target_table) = lower(isc.table_schema||'.'||isc.table_name) WHERE uem.work_table is NOT NULL AND uem.active_flag = TRUE AND lower(isc.table_schema) = 'udl' AND uem.source_table_type = lower('" +coreOrZeus + "')"
			}).execute();

			snowflake.execute({
				sqlText: "begin transaction"
			});

			try {
				while (rsGetTables.next()) {

					var tgtTable = rsGetTables.getColumnValue(1);
					var wrkTable = rsGetTables.getColumnValue(2);
					var histTable = rsGetTables.getColumnValue(3);
					var tgtColumns = rsGetTables.getColumnValue(4);

					var processRun = {};
					processRun.batch_run_id = batchRunId;
					processRun.process_name = "Work To Target: "+ histTable;
					processRun.process_name_internal = "prc_udl_publish_to_target:"+wrkTable+":"+histTable;
					processRun.process_target = histTable;
					processRun.process_source = wrkTable;
					processRun.action = "initiate";

					var rsInitiateProcess = snowflake.createStatement({
						sqlText: "CALL udl_batch_process.prc_udl_process_handler(:1)",
						binds: [JSON.stringify(processRun)]
					}).execute();

					if (rsInitiateProcess.next()){
						var returnStatus = rsInitiateProcess.getColumnValue(1);

						if (returnStatus.status =="success") {
							var processRunId = returnStatus.process_run_id;
						} else {
							throw new Error(returnStatus.err_code, returnStatus.err_msg);
						}
					}

					var processRun = {};
					processRun.batch_run_id = batchRunId;
					processRun.process_run_id = processRunId;
					processRun.action = "complete";
					var process_running_in_full_load_list = []
					var specific_entity_full_load = false;
					if (process_running_in_full_load && process_running_in_full_load.trim() !== "") {
						process_running_in_full_load_list = process_running_in_full_load.toLowerCase().split(",").map(function (item) {return item.trim();});
						specific_entity_full_load = true
					}

					if (fullLoad || (specific_entity_full_load && process_running_in_full_load_list.includes(tgtTable))){
					    if(coreOrZeus === "integration"){
					        var rsDeleteQuery = snowflake.createStatement({
					            sqlText: "DELETE FROM "+histTable+" AS tgt WHERE tgt.id != -1 "
					        }).execute();
					    }
					}
					else {
						var rsDeleteQuery = snowflake.createStatement({
							sqlText: "DELETE FROM "+histTable+" AS tgt USING "+wrkTable+" AS wrk WHERE tgt.id = wrk.id"
						}).execute();

						if (rsDeleteQuery.next()){
							processRun.no_of_records_updated = rsDeleteQuery.getColumnValue(1);
						}
					}

					var rsInsertQuery = snowflake.createStatement({
						sqlText: "INSERT INTO "+histTable+"("+tgtColumns+") SELECT "+tgtColumns+" FROM "+wrkTable
					}).execute();

					if(histTable !== tgtTable){
					    var rsDeleteQuery2 = snowflake.createStatement({
					                            sqlText: "TRUNCATE TABLE "+tgtTable+""
					    }).execute();

					    var rsInsertQueryHistToTgt = snowflake.createStatement({
					                            sqlText: "INSERT INTO "+tgtTable+"("+tgtColumns+") SELECT "+tgtColumns+" FROM "+histTable+" WHERE active_flag = true ORDER BY tenant_code"
					    }).execute();
					}

					if (rsInsertQuery.next()){
						processRun.no_of_records_inserted = rsInsertQuery.getColumnValue(1);
					}

					if (fullLoad) {
						processRun.no_of_records_processed = processRun.no_of_records_inserted;
					}
					else {
						processRun.no_of_records_processed = processRun.no_of_records_updated + processRun.no_of_records_inserted;
					}
					
					snowflake.execute({
						sqlText: "CALL udl_batch_process.prc_udl_process_handler(:1)", binds: [JSON.stringify(processRun)]
					});
					
				}
			} catch(err) {
				snowflake.execute({
				sqlText: "rollback"
			});
			throw err;
			}
			snowflake.execute({
				sqlText: "commit"
			});
		} else {
			throw new Error("E1400", "Mandatory parameter missing: batch_run_id");
		}
		return {"status":"success","batch_run_id":batchRunId};
	} catch(err) {
		if (!err.err_code) {
			var err = new Error('E0000',err.toString());
		}
		var logMsg = new LogMessage("BatchRunId_"+batchRunId, "ERROR", err.err_code, err)
		snowflake.execute({
			sqlText: "CALL log.prc_logging(:1)",
			binds: [JSON.stringify(logMsg)]
		});
		logMsg.status = "error";
		logMsg.batch_run_id = batchRunId;

		if (processRunId) {
			var processRun = {};
			processRun.batch_run_id = batchRunId;
			processRun.process_run_id = processRunId;
			processRun.action = "error";
			processRun.process_error_message = JSON.stringify(err);
			snowflake.execute({
				sqlText: "CALL udl_batch_process.prc_udl_process_handler(:1)",
				binds: [JSON.stringify(processRun)]
			});
		}

	    return logMsg;
	}
	
	$$
	;

