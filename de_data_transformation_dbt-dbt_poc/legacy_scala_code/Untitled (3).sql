CREATE OR REPLACE PROCEDURE udl_batch_process.prc_udl_batch_handler(params_json varchar)
	RETURNS VARIANT
	LANGUAGE JAVASCRIPT
	STRICT
	EXECUTE AS OWNER
	AS
	$$

	/*
	*	Procedure Name	:	udl_batch_process.prc_udl_batch_handler
	*	Description		:	This is a wrapper procedure to perform operations over execution_run_stats.batch_run table. It identifies and persists the execution stats for UDL data load.
	*	Arguments		:	params_json, varchar. Values should be provided in JSON format. It expects following attributes:
	*						- action, string (Allowed Values: initiate, complete, error)
	*						- batch_name, string (Should be provided from config.batch_config. Mandatory for action = initiate)
	*						- batch_run_id, number (Mandatory for action = complete or error)
	*	Returns			: 	Status and Batch_Run_Id in JSON format
	*	Updates			:	Handle Specific Entity Full Load, Ishank Gupta, 01-Sep-2023
	*					:	Add support for simpplr_scheduled_reports_generate
	*/

	class LogMessage {
		constructor(t_id, severity, code, msg) {
			this.process_name = "udl_data_load";
			this.sub_process_name = "prc_udl_batch_handler";
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
		if (paramValues.action) {
			if (paramValues.action == "initiate") {
				if (paramValues.batch_name) {
					var rsCheckExisting = snowflake.createStatement({
						sqlText: "SELECT iff(count(1)=0,FALSE, TRUE) running_flag FROM execution_run_stats.batch_run WHERE active_flag = TRUE and batch_config_id NOT IN (4,5,96,6,95,7,8,94,9,93,10,11,92,12,13,91,14,90,15,89)",
						binds: [paramValues.batch_name]
					}).execute();

                    var nlCheckExisting = snowflake.createStatement({
					sqlText: "SELECT iff(count(1)=0,FALSE, TRUE) running_flag FROM execution_run_stats.batch_run WHERE active_flag = TRUE and batch_config_id IN (5,96)",
						binds: [paramValues.batch_name]
					}).execute();

                    var vaCheckExisting = snowflake.createStatement({
						sqlText: "SELECT iff(count(1)=0,FALSE, TRUE) running_flag FROM execution_run_stats.batch_run WHERE active_flag = TRUE and batch_config_id IN (6,95)",
						binds: [paramValues.batch_name]
					}).execute();

                    var zcCheckExisting = snowflake.createStatement({
						sqlText: "SELECT iff(count(1)=0,FALSE, TRUE) running_flag FROM execution_run_stats.batch_run WHERE active_flag = TRUE and batch_config_id IN (8,94)",
						binds: [paramValues.batch_name]
					}).execute();

                    var elsCheckExisting = snowflake.createStatement({
						sqlText: "SELECT iff(count(1)=0,FALSE, TRUE) running_flag FROM execution_run_stats.batch_run WHERE active_flag = TRUE and batch_config_id IN (9,93)",
						binds: [paramValues.batch_name]
					}).execute();

                    var dsCheckExisting = snowflake.createStatement({
                        sqlText: "SELECT iff(count(1)=0,FALSE, TRUE) running_flag FROM execution_run_stats.batch_run WHERE active_flag = TRUE and batch_config_id IN (7,10)",
                        binds: [paramValues.batch_name]
                    }).execute();

                    var zaCheckExisting = snowflake.createStatement({
                        sqlText: "SELECT iff(count(1)=0,FALSE, TRUE) running_flag FROM execution_run_stats.batch_run WHERE active_flag = TRUE and batch_config_id IN (11,92)",
                        binds: [paramValues.batch_name]
                    }).execute();

                    var srCheckExisting = snowflake.createStatement({
                        sqlText: "SELECT iff(count(1)=0,FALSE, TRUE) running_flag FROM execution_run_stats.batch_run WHERE active_flag = TRUE and batch_config_id IN (12)",
                        binds: [paramValues.batch_name]
                    }).execute();

                    var intCheckExisting = snowflake.createStatement({
						sqlText: "SELECT iff(count(1)=0,FALSE, TRUE) running_flag FROM execution_run_stats.batch_run WHERE active_flag = TRUE and batch_config_id IN (13,91)",
						binds: [paramValues.batch_name]
					}).execute();

                    var formCheckExisting = snowflake.createStatement({
						sqlText: "SELECT iff(count(1)=0,FALSE, TRUE) running_flag FROM execution_run_stats.batch_run WHERE active_flag = TRUE and batch_config_id IN (14,90)",
						binds: [paramValues.batch_name]
					}).execute();

                    var pollCheckExisting = snowflake.createStatement({
						sqlText: "SELECT iff(count(1)=0,FALSE, TRUE) running_flag FROM execution_run_stats.batch_run WHERE active_flag = TRUE and batch_config_id IN (15,89)",
						binds: [paramValues.batch_name]
					}).execute();

					if (rsCheckExisting.next()) {
						var existingRun = rsCheckExisting.getColumnValue(1);
						if (paramValues.batch_name == "simpplr_monthly_snapshots_warehouse_load") {
							existingRun = false;
}
                        if (paramValues.batch_name == "simpplr_employee_newsletter_warehouse_load" || paramValues.batch_name == "simpplr_employee_newsletter_initial_load") {
							nlCheckExisting.next()
                            existingRun = nlCheckExisting.getColumnValue(1);
}
                        if (paramValues.batch_name == "simpplr_virtual_assistant_warehouse_load" || paramValues.batch_name == "simpplr_virtual_assistant_initial_load") {
							vaCheckExisting.next()
                            existingRun = vaCheckExisting.getColumnValue(1);
}
                        if (paramValues.batch_name == "simpplr_zeus_chat_warehouse_load" || paramValues.batch_name == "simpplr_zeus_chat_initial_load") {
							zcCheckExisting.next()
                            existingRun = zcCheckExisting.getColumnValue(1);
}
						if (paramValues.batch_name == "simpplr_employee_listening_warehouse_load" || paramValues.batch_name == "simpplr_employee_listening_initial_load") {
							elsCheckExisting.next()
                            existingRun = elsCheckExisting.getColumnValue(1);
}
                        if (paramValues.batch_name == "simpplr_daily_snapshots_warehouse_load" || paramValues.batch_name == "simpplr_daily_snapshots_initial_load") {
                            dsCheckExisting.next()
                            existingRun = dsCheckExisting.getColumnValue(1);
}
                        if (paramValues.batch_name == "simpplr_zeus_aqua_warehouse_load" || paramValues.batch_name == "simpplr_zeus_aqua_initial_load") {
                            zaCheckExisting.next()
                            existingRun = zaCheckExisting.getColumnValue(1);
}
                        if (paramValues.batch_name == "simpplr_scheduled_reports_generate") {
                            srCheckExisting.next()
                            existingRun = srCheckExisting.getColumnValue(1);
}
                        if (paramValues.batch_name == "simpplr_integration_warehouse_load" || paramValues.batch_name == "simpplr_integration_initial_load") {
							intCheckExisting.next()
                            existingRun = intCheckExisting.getColumnValue(1);
}
						if (paramValues.batch_name == "simpplr_form_warehouse_load" || paramValues.batch_name == "simpplr_form_initial_load") {
							formCheckExisting.next()
                            existingRun = formCheckExisting.getColumnValue(1);
}
                        if (paramValues.batch_name == "simpplr_poll_warehouse_load" || paramValues.batch_name == "simpplr_poll_initial_load") {
                            pollCheckExisting.next()
                            existingRun = pollCheckExisting.getColumnValue(1);
}
					}
					if (existingRun) {
						throw new Error("E1100", "Existing Run in Progress");
} else {
						var rsGetBatchRunDetails = snowflake.createStatement({
							sqlText: "SELECT execution_run_stats.seq_batch_run_id.nextval AS batch_run_id, batch_id, notes, full_load, core_or_zeus FROM config.batch_config WHERE batch_name = :1 AND active_flag = TRUE",
							binds: [paramValues.batch_name]
						}).execute();

						if (rsGetBatchRunDetails.next()) {
							var batchRunId = rsGetBatchRunDetails.getColumnValue('BATCH_RUN_ID');
							var batchConfigId = rsGetBatchRunDetails.getColumnValue('BATCH_ID');
							var batchNotes = rsGetBatchRunDetails.getColumnValue('NOTES');
							var fullLoad = rsGetBatchRunDetails.getColumnValue('FULL_LOAD');
							var coreOrZeus = rsGetBatchRunDetails.getColumnValue('CORE_OR_ZEUS');
} else {
							throw new Error("E1110", "Invalid batch name");
}

						snowflake.execute({
							sqlText: "begin transaction"
						});

						snowflake.execute({
							sqlText: "INSERT INTO execution_run_stats.batch_run(batch_run_id, batch_config_id, batch_name, batch_notes, core_or_zeus, process_running_in_full_load, full_load, batch_start_time, batch_status, active_flag, data_process_start_time, data_process_end_time) SELECT  :1, :2, :3, :4, :5, :6, "+fullLoad+", current_timestamp, 'IN_PROGRESS', TRUE, CASE WHEN "+fullLoad+" THEN to_timestamp('01-Jan-2000') WHEN '"+paramValues.batch_name+"'='simpplr_monthly_snapshots_warehouse_load' THEN to_timestamp(DATEADD(MONTH, -1, CURRENT_DATE())) WHEN '"+paramValues.batch_name+"'='simpplr_aggregate_warehouse_load' OR '"+paramValues.batch_name+"'='simpplr_aggregate_initial_load' THEN (SELECT MAX(BATCH_START_TIME) FROM execution_run_stats.batch_run WHERE core_or_zeus = 'aggregate' AND batch_status = 'SUCCESS') ELSE nvl(max(data_process_end_time),to_timestamp('01-Jan-2000')) END, current_timestamp FROM execution_run_stats.batch_run WHERE batch_status = 'SUCCESS' and nvl(core_or_zeus,'core') = :5",
							binds: [batchRunId, batchConfigId, paramValues.batch_name, batchNotes, coreOrZeus, paramValues.process_running_in_full_load]
						});

						var processRun = {};
						processRun.action = "complete";
						processRun.batch_run_id = batchRunId;
						processRun.process_name = "New batch initiated";
						processRun.process_name_internal = "prc_udl_batch_handler:initiate";

						snowflake.execute({
							sqlText: "CALL udl_batch_process.prc_udl_process_handler(:1)",
							binds: [JSON.stringify(processRun)]
						});

						snowflake.execute({
							sqlText: "commit"
						});
}
				} else {
					throw new Error("E1120", "Mandatory parameter missing: batch_name");
}
			} else if (paramValues.action == "complete" || paramValues.action == "error") {
				if (paramValues.batch_run_id) {
					var batchRunId = paramValues.batch_run_id;
					var rsCheckExisting = snowflake.createStatement({
						sqlText: "SELECT iff(count(1)=1,TRUE, FALSE) running_flag FROM execution_run_stats.batch_run WHERE batch_run_id = :1 AND active_flag = TRUE",
						binds: [batchRunId]
					}).execute();

					if (rsCheckExisting.next()) {
						var existingRun = rsCheckExisting.getColumnValue(1);
						if (existingRun) {

							var updateQuery = "UPDATE execution_run_stats.batch_run SET batch_status = "
							var processRun = {};
							processRun.action = "complete";
							processRun.batch_run_id = batchRunId;

							if (paramValues.action == "complete") {
								updateQuery += "'SUCCESS'"
								processRun.process_name = "Batch run completed successfully";
								processRun.process_name_internal = "prc_udl_batch_handler:complete";
} else {
								updateQuery += "'ERROR'"
								processRun.process_name = "Batch run completed with error";
								processRun.process_name_internal = "prc_udl_batch_handler:error";
}

							updateQuery += ", active_flag = FALSE, batch_end_time = current_timestamp, updated_date = current_timestamp, updated_by = current_user WHERE batch_run_id = :1";

							snowflake.execute({
								sqlText: "begin transaction"
							});

							snowflake.execute({
								sqlText: updateQuery,
								binds: [batchRunId]
							});

							snowflake.execute({
								sqlText: "CALL udl_batch_process.prc_udl_process_handler(:1)",
								binds: [JSON.stringify(processRun)]
							});

							snowflake.execute({
								sqlText: "commit"
							});
} else {
							throw new Error("E1130", "Invalid batch_run_id");
}
					}
				} else {
					throw new Error("E1140", "Mandatory parameter missing: batch_run_id");
}
			} else {
				throw new Error("E1150", "Invalid parameter value for action (Allowed Values: initiate, complete, error)");
}
			return {"status":"success", "batch_run_id":batchRunId};
} else {
			throw new Error("E1160", "Mandatory parameter missing: action");
}

	} catch (err) {
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

return logMsg;
}
	$$
	; 