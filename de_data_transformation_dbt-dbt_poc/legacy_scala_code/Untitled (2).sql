CREATE OR REPLACE PROCEDURE udl_batch_process.prc_udl_manage_transient_objects(params_json varchar)
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

	snowflake.execute({
		sqlText: "begin transaction"
	});
	try {

		try {
			var paramValues = JSON.parse(PARAMS_JSON);
		} catch (err) {
			throw new Error("E1000", "Invalid JSON passed in parameter");
		}

		if (paramValues.batch_run_id) {

			var batchRunId = paramValues.batch_run_id;
			var rsCheckExisting = snowflake.createStatement({
				sqlText: "SELECT iff(count(1)=1,TRUE, FALSE) running_flag, lower(max(core_or_zeus)) FROM execution_run_stats.batch_run WHERE batch_run_id = :1 AND active_flag = TRUE",
				binds: [batchRunId]
			}).execute();

			if (rsCheckExisting.next()) {
				var existingRun = rsCheckExisting.getColumnValue(1);
				var coreOrZeus = rsCheckExisting.getColumnValue(2);
				if (existingRun) {

					var processRun = {};
					processRun.batch_run_id = batchRunId;
					processRun.process_name = "Clean up transient objects since last run";
					processRun.process_name_internal = "prc_udl_manage_transient_objects";
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
							throw new Error(returnStatus.log_code, returnStatus.log_description);
						}
					}


					var rsGetStagingTables = snowflake.createStatement({
						sqlText: "SELECT DISTINCT lower(staging_table) FROM config.udl_entity_mapping WHERE active_flag = TRUE AND lower(staging_table) LIKE 'udl_batch_process.%' AND lower(target_table) IN (SELECT lower(table_schema||'.'||table_name) from information_schema.tables WHERE lower(table_schema) = 'udl') AND lower(source_table_type) = lower('" +coreOrZeus + "')"
					}).execute();
					
					while (rsGetStagingTables.next()) {

						snowflake.execute({
							sqlText: "DROP TABLE IF EXISTS "+rsGetStagingTables.getColumnValue(1)
						});
					}
					
					var rsGetWorkTables = snowflake.createStatement({
						sqlText: "SELECT DISTINCT lower(work_table), lower(target_table) FROM config.udl_entity_mapping WHERE active_flag = TRUE AND lower(work_table) LIKE 'udl_batch_process.%' AND lower(target_table) IN (SELECT lower(table_schema||'.'||table_name) from information_schema.tables WHERE lower(table_schema) = 'udl') AND lower(target_table) NOT LIKE '%_measures' AND lower(source_table_type) = lower('" +coreOrZeus + "')"
					}).execute();
					
					while (rsGetWorkTables.next()) {
						
						snowflake.execute({
							sqlText: "CREATE OR REPLACE TRANSIENT TABLE "+rsGetWorkTables.getColumnValue(1)+" AS SELECT * FROM "+rsGetWorkTables.getColumnValue(2) +" WHERE 1=2 "
						});

					}

					var rsGetMeasuresWorkTables = snowflake.createStatement({
						sqlText: "SELECT DISTINCT lower(work_table), lower(target_table) FROM config.udl_entity_mapping WHERE active_flag = TRUE AND lower(work_table) LIKE 'udl_batch_process.%' AND lower(target_table) IN (SELECT lower(table_schema||'.'||table_name) from information_schema.tables WHERE lower(table_schema) = 'udl') AND lower(target_table) LIKE '%_measures' AND lower(source_table_type) = lower('" +coreOrZeus + "')"
					}).execute();
					
					while (rsGetMeasuresWorkTables.next()) {
						
						snowflake.execute({
							sqlText: "CREATE OR REPLACE TABLE "+rsGetMeasuresWorkTables.getColumnValue(1)+" AS SELECT * FROM "+rsGetMeasuresWorkTables.getColumnValue(2) +" WHERE 1=2 "
						});

					}

					var processRun = {};
					processRun.batch_run_id = batchRunId;
					processRun.process_run_id = processRunId;
					processRun.process_name_internal = "prc_udl_manage_transient_objects";
					processRun.action = "complete";

					snowflake.execute({
						sqlText: "CALL udl_batch_process.prc_udl_process_handler(:1)",
						binds: [JSON.stringify(processRun)]
					});

                    if (coreOrZeus == "core") {
                    snowflake.execute({
                    sqlText: "DELETE FROM udl_batch_process.records_to_process WHERE lower(type) = 'ref_to_ref'"
                    });
                    }

                    snowflake.execute({
                    sqlText: "commit"
                    });

					return {"status":"success", "batch_run_id": batchRunId}
				} else {
			throw new Error("E1210", "Invalid batch_run_id");
		}
			}
		} else {
			throw new Error("E1200", "Mandatory parameter missing: batch_run_id");
		}			
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
			processRun.process_error_message = JSON.stringify(err);
			processRun.action = "error";
			snowflake.execute({
				sqlText: "CALL udl_batch_process.prc_udl_process_handler(:1)",
				binds: [JSON.stringify(processRun)]
			});
		}

	    return logMsg;
	}
	snowflake.execute({
		sqlText: "commit"
	});
	
	$$
	;