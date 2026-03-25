-- =====================================================
-- DBT BATCH CONTROL - SETUP SCRIPT  
-- Procedures in DBT_CONTROL schema
-- Tables in EXECUTION_RUN_STATS schema
-- =====================================================

USE ROLE R_DEPLOYMENT_ADMIN_DEV;
USE DATABASE COMMON_TENANT_DEV;
USE WAREHOUSE WH_ETL_DEV;

-- =====================================================
-- 1. PRC_DBT_BATCH_HANDLER (in DBT_CONTROL schema)
-- Actions: initiate, complete, error
-- =====================================================
CREATE OR REPLACE PROCEDURE DBT_CONTROL.PRC_DBT_BATCH_HANDLER(PARAMS_JSON VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    var params = JSON.parse(PARAMS_JSON);
    if (!params.action) throw new Error("Missing: action");
    
    if (params.action === 'initiate') {
        if (!params.batch_name) throw new Error("Missing: batch_name");
        
        var checkSql = "SELECT BATCH_RUN_ID FROM EXECUTION_RUN_STATS.DBT_BATCH_RUN WHERE ACTIVE_FLAG = TRUE LIMIT 1";
        var checkRes = snowflake.createStatement({sqlText: checkSql}).execute();
        if (checkRes.next()) throw new Error("Batch already running: " + checkRes.getColumnValue(1));
        
        var batchConfigSql = "SELECT BATCH_ID FROM CONFIG.DBT_BATCH_CONFIG WHERE BATCH_NAME = :1 AND ACTIVE_FLAG = TRUE";
        var batchConfigRes = snowflake.createStatement({sqlText: batchConfigSql, binds: [params.batch_name]}).execute();
        var batchConfigId = 1;
        if (batchConfigRes.next()) batchConfigId = batchConfigRes.getColumnValue(1);
        
        var isFullLoad = params.full_load || false;
        var fullLoadTrans = params.process_running_in_full_load || '';
        
        var wmStart = '2000-01-01 00:00:00';
        if (!isFullLoad) {
            var wmSql = "SELECT TO_VARCHAR(COALESCE(MAX(DATA_PROCESS_END_TIME), '2000-01-01'::TIMESTAMP_NTZ), 'YYYY-MM-DD HH24:MI:SS') FROM EXECUTION_RUN_STATS.DBT_BATCH_RUN WHERE BATCH_STATUS IN ('COMPLETED', 'COMPLETED_WITH_ERRORS')";
            var wmRes = snowflake.createStatement({sqlText: wmSql}).execute();
            if (wmRes.next() && wmRes.getColumnValue(1)) wmStart = wmRes.getColumnValue(1);
        }
        
        var insSql = `INSERT INTO EXECUTION_RUN_STATS.DBT_BATCH_RUN 
            (BATCH_CONFIG_ID, BATCH_NAME, BATCH_STATUS, ACTIVE_FLAG, BATCH_START_TIME, DATA_PROCESS_START_TIME, DATA_PROCESS_END_TIME, FULL_LOAD, PROCESS_RUNNING_IN_FULL_LOAD, CREATED_BY)
            VALUES (:1, :2, 'RUNNING', TRUE, CURRENT_TIMESTAMP(), :3::TIMESTAMP_NTZ, CURRENT_TIMESTAMP(), :4, :5, CURRENT_USER())`;
        snowflake.createStatement({sqlText: insSql, binds: [batchConfigId, params.batch_name, wmStart, isFullLoad, fullLoadTrans]}).execute();
        snowflake.createStatement({sqlText: "COMMIT"}).execute();
        
        var idSql = "SELECT BATCH_RUN_ID, DATA_PROCESS_START_TIME, DATA_PROCESS_END_TIME FROM EXECUTION_RUN_STATS.DBT_BATCH_RUN WHERE ACTIVE_FLAG = TRUE";
        var idRes = snowflake.createStatement({sqlText: idSql}).execute();
        idRes.next();
        
        return {
            status: "success", 
            action: "initiate", 
            batch_run_id: idRes.getColumnValue(1), 
            full_load: isFullLoad, 
            data_process_start_time: idRes.getColumnValue(2),
            data_process_end_time: idRes.getColumnValue(3)
        };
        
    } else if (params.action === 'complete') {
        if (!params.batch_run_id) throw new Error("Missing: batch_run_id");
        
        var checkFailedSql = "SELECT COUNT(*) FROM EXECUTION_RUN_STATS.DBT_PROCESS_RUN WHERE BATCH_RUN_ID = :1 AND PROCESS_STATUS = 'FAILED'";
        var checkFailedRes = snowflake.createStatement({sqlText: checkFailedSql, binds: [params.batch_run_id]}).execute();
        checkFailedRes.next();
        var failedCount = checkFailedRes.getColumnValue(1);
        
        var finalStatus = failedCount > 0 ? 'COMPLETED_WITH_ERRORS' : 'COMPLETED';
        var updSql = `UPDATE EXECUTION_RUN_STATS.DBT_BATCH_RUN 
            SET BATCH_STATUS = :1, ACTIVE_FLAG = FALSE, BATCH_END_TIME = CURRENT_TIMESTAMP(), 
                UPDATED_DATE = CURRENT_TIMESTAMP(), UPDATED_BY = CURRENT_USER() 
            WHERE BATCH_RUN_ID = :2`;
        snowflake.createStatement({sqlText: updSql, binds: [finalStatus, params.batch_run_id]}).execute();
        snowflake.createStatement({sqlText: "COMMIT"}).execute();
        
        return {status: "success", action: "complete", batch_run_id: params.batch_run_id, failed_count: failedCount, final_status: finalStatus};
        
    } else if (params.action === 'error') {
        if (!params.batch_run_id) throw new Error("Missing: batch_run_id");
        var errMsg = (params.error_message || '').substring(0, 4000);
        var errSql = `UPDATE EXECUTION_RUN_STATS.DBT_BATCH_RUN 
            SET BATCH_STATUS = 'FAILED', ACTIVE_FLAG = FALSE, BATCH_END_TIME = CURRENT_TIMESTAMP(), 
                ERROR_MESSAGE = :1, UPDATED_DATE = CURRENT_TIMESTAMP(), UPDATED_BY = CURRENT_USER() 
            WHERE BATCH_RUN_ID = :2`;
        snowflake.createStatement({sqlText: errSql, binds: [errMsg, params.batch_run_id]}).execute();
        snowflake.createStatement({sqlText: "COMMIT"}).execute();
        
        return {status: "success", action: "error", batch_run_id: params.batch_run_id};
        
    } else {
        throw new Error("Invalid action. Allowed: initiate, complete, error");
    }
} catch (err) {
    return {status: "error", error: err.message};
}
$$;


-- =====================================================
-- 2. PRC_DBT_MANAGE_TRANSIENT_OBJECTS (in DBT_CONTROL schema)
-- Creates empty STG and WRK tables from target schema
-- =====================================================
CREATE OR REPLACE PROCEDURE DBT_CONTROL.PRC_DBT_MANAGE_TRANSIENT_OBJECTS(PARAMS_JSON VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    var params = JSON.parse(PARAMS_JSON);
    if (!params.batch_run_id) throw new Error("Missing: batch_run_id");
    
    var batchRunId = params.batch_run_id;
    
    var checkSql = "SELECT BATCH_RUN_ID FROM EXECUTION_RUN_STATS.DBT_BATCH_RUN WHERE BATCH_RUN_ID = :1 AND ACTIVE_FLAG = TRUE";
    var checkRes = snowflake.createStatement({sqlText: checkSql, binds: [batchRunId]}).execute();
    if (!checkRes.next()) throw new Error("No active batch found for batch_run_id: " + batchRunId);
    
    var processRunSql = `INSERT INTO EXECUTION_RUN_STATS.DBT_PROCESS_RUN 
        (BATCH_RUN_ID, ENTITY_NAME, PROCESS_STEP, PROCESS_ORDER, PROCESS_STATUS, PROCESS_START_TIME, CREATED_BY)
        VALUES (:1, 'transient_objects', 'manage_transient_objects', 0, 'RUNNING', CURRENT_TIMESTAMP(), CURRENT_USER())`;
    snowflake.createStatement({sqlText: processRunSql, binds: [batchRunId]}).execute();
    
    var tablesCreated = 0;
    var tablesDropped = 0;
    
    var getStagingSql = `SELECT STAGING_TABLE, TARGET_TABLE 
        FROM CONFIG.DBT_ENTITY_MAPPING 
        WHERE ACTIVE_FLAG = TRUE AND STAGING_TABLE IS NOT NULL`;
    var stgRes = snowflake.createStatement({sqlText: getStagingSql}).execute();
    
    while (stgRes.next()) {
        var stgTable = stgRes.getColumnValue(1);
        var tgtTable = stgRes.getColumnValue(2);
        
        try {
            snowflake.createStatement({sqlText: "DROP TABLE IF EXISTS " + stgTable}).execute();
            tablesDropped++;
        } catch (e) {}
        
        try {
            var createSql = "CREATE OR REPLACE TRANSIENT TABLE " + stgTable + " AS SELECT * FROM " + tgtTable + " WHERE 1=2";
            snowflake.createStatement({sqlText: createSql}).execute();
            tablesCreated++;
        } catch (e) {}
    }
    
    var getWorkSql = `SELECT WORK_TABLE, TARGET_TABLE 
        FROM CONFIG.DBT_ENTITY_MAPPING 
        WHERE ACTIVE_FLAG = TRUE AND WORK_TABLE IS NOT NULL`;
    var wrkRes = snowflake.createStatement({sqlText: getWorkSql}).execute();
    
    while (wrkRes.next()) {
        var wrkTable = wrkRes.getColumnValue(1);
        var tgtTable = wrkRes.getColumnValue(2);
        
        try {
            snowflake.createStatement({sqlText: "DROP TABLE IF EXISTS " + wrkTable}).execute();
            tablesDropped++;
        } catch (e) {}
        
        try {
            var createSql = "CREATE OR REPLACE TRANSIENT TABLE " + wrkTable + " AS SELECT * FROM " + tgtTable + " WHERE 1=2";
            snowflake.createStatement({sqlText: createSql}).execute();
            tablesCreated++;
        } catch (e) {}
    }
    
    var updateSql = `UPDATE EXECUTION_RUN_STATS.DBT_PROCESS_RUN
        SET PROCESS_STATUS = 'COMPLETED', PROCESS_END_TIME = CURRENT_TIMESTAMP(),
            PROCESS_DURATION_SECONDS = DATEDIFF('SECOND', PROCESS_START_TIME, CURRENT_TIMESTAMP()),
            ROWS_PROCESSED = :1, PROCESS_NOTES = :2,
            UPDATED_DATE = CURRENT_TIMESTAMP(), UPDATED_BY = CURRENT_USER()
        WHERE BATCH_RUN_ID = :3 AND PROCESS_STEP = 'manage_transient_objects' AND PROCESS_STATUS = 'RUNNING'`;
    var notes = 'Created: ' + tablesCreated + ', Dropped: ' + tablesDropped;
    snowflake.createStatement({sqlText: updateSql, binds: [tablesCreated, notes, batchRunId]}).execute();
    
    snowflake.createStatement({sqlText: "COMMIT"}).execute();
    
    return {status: "success", batch_run_id: batchRunId, tables_created: tablesCreated, tables_dropped: tablesDropped};
    
} catch (err) {
    return {status: "error", error: err.message};
}
$$;


-- =====================================================
-- 3. PRC_DBT_PROCESS_HANDLER (in DBT_CONTROL schema)
-- Actions: initiate, complete, error, skip
-- =====================================================
CREATE OR REPLACE PROCEDURE DBT_CONTROL.PRC_DBT_PROCESS_HANDLER(PARAMS_JSON VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    var params = JSON.parse(PARAMS_JSON);
    if (!params.action) throw new Error("Missing: action");
    if (!params.batch_run_id) throw new Error("Missing: batch_run_id");
    
    var batchRunId = params.batch_run_id;
    
    if (params.action === 'initiate') {
        if (!params.entity_name || !params.process_step) throw new Error("Missing: entity_name or process_step");
        
        var orderSql = "SELECT COALESCE(MAX(PROCESS_ORDER), 0) + 1 FROM EXECUTION_RUN_STATS.DBT_PROCESS_RUN WHERE BATCH_RUN_ID = :1";
        var orderRes = snowflake.createStatement({sqlText: orderSql, binds: [batchRunId]}).execute();
        orderRes.next();
        var processOrder = orderRes.getColumnValue(1);
        
        var entityIdSql = "SELECT ID FROM CONFIG.DBT_ENTITY_MAPPING WHERE DBT_MODEL_NAME = :1 AND ACTIVE_FLAG = TRUE LIMIT 1";
        var entityIdRes = snowflake.createStatement({sqlText: entityIdSql, binds: [params.entity_name]}).execute();
        var entityId = entityIdRes.next() ? entityIdRes.getColumnValue(1) : null;
        
        var insSql = `INSERT INTO EXECUTION_RUN_STATS.DBT_PROCESS_RUN 
            (BATCH_RUN_ID, ENTITY_ID, ENTITY_NAME, PROCESS_STEP, PROCESS_ORDER, PROCESS_STATUS, PROCESS_START_TIME, CREATED_BY)
            VALUES (:1, :2, :3, :4, :5, 'RUNNING', CURRENT_TIMESTAMP(), CURRENT_USER())`;
        snowflake.createStatement({sqlText: insSql, binds: [batchRunId, entityId, params.entity_name, params.process_step, processOrder]}).execute();
        
        var idSql = "SELECT MAX(PROCESS_RUN_ID) FROM EXECUTION_RUN_STATS.DBT_PROCESS_RUN WHERE BATCH_RUN_ID = :1 AND ENTITY_NAME = :2";
        var idRes = snowflake.createStatement({sqlText: idSql, binds: [batchRunId, params.entity_name]}).execute();
        idRes.next();
        
        snowflake.createStatement({sqlText: "COMMIT"}).execute();
        return {status: "success", action: "initiate", process_run_id: idRes.getColumnValue(1)};
        
    } else if (params.action === 'complete') {
        if (!params.process_run_id && !params.entity_name) throw new Error("Missing: process_run_id or entity_name");
        
        var rowsProcessed = params.rows_processed || 0;
        var rowsInserted = params.rows_inserted || 0;
        var rowsUpdated = params.rows_updated || 0;
        
        var updSql = `UPDATE EXECUTION_RUN_STATS.DBT_PROCESS_RUN
            SET PROCESS_STATUS = 'COMPLETED', PROCESS_END_TIME = CURRENT_TIMESTAMP(),
                PROCESS_DURATION_SECONDS = DATEDIFF('SECOND', PROCESS_START_TIME, CURRENT_TIMESTAMP()),
                ROWS_PROCESSED = :1, ROWS_INSERTED = :2, ROWS_UPDATED = :3,
                UPDATED_DATE = CURRENT_TIMESTAMP(), UPDATED_BY = CURRENT_USER()
            WHERE BATCH_RUN_ID = :4 AND PROCESS_STATUS = 'RUNNING'`;
        
        if (params.process_run_id) {
            updSql += " AND PROCESS_RUN_ID = :5";
            snowflake.createStatement({sqlText: updSql, binds: [rowsProcessed, rowsInserted, rowsUpdated, batchRunId, params.process_run_id]}).execute();
        } else {
            updSql += " AND ENTITY_NAME = :5";
            snowflake.createStatement({sqlText: updSql, binds: [rowsProcessed, rowsInserted, rowsUpdated, batchRunId, params.entity_name]}).execute();
        }
        
        snowflake.createStatement({sqlText: "COMMIT"}).execute();
        return {status: "success", action: "complete"};
        
    } else if (params.action === 'error') {
        var errMsg = (params.error_message || '').substring(0, 4000);
        var errSql = `UPDATE EXECUTION_RUN_STATS.DBT_PROCESS_RUN
            SET PROCESS_STATUS = 'FAILED', PROCESS_END_TIME = CURRENT_TIMESTAMP(),
                PROCESS_DURATION_SECONDS = DATEDIFF('SECOND', PROCESS_START_TIME, CURRENT_TIMESTAMP()),
                ERROR_MESSAGE = :1, UPDATED_DATE = CURRENT_TIMESTAMP(), UPDATED_BY = CURRENT_USER()
            WHERE BATCH_RUN_ID = :2 AND PROCESS_STATUS = 'RUNNING'`;
        snowflake.createStatement({sqlText: errSql, binds: [errMsg, batchRunId]}).execute();
        snowflake.createStatement({sqlText: "COMMIT"}).execute();
        return {status: "success", action: "error"};
        
    } else if (params.action === 'skip') {
        if (!params.entity_name || !params.process_step) throw new Error("Missing: entity_name or process_step");
        
        var orderSql = "SELECT COALESCE(MAX(PROCESS_ORDER), 0) + 1 FROM EXECUTION_RUN_STATS.DBT_PROCESS_RUN WHERE BATCH_RUN_ID = :1";
        var orderRes = snowflake.createStatement({sqlText: orderSql, binds: [batchRunId]}).execute();
        orderRes.next();
        
        var skipReason = params.skip_reason || 'No data to process';
        var skipSql = `INSERT INTO EXECUTION_RUN_STATS.DBT_PROCESS_RUN 
            (BATCH_RUN_ID, ENTITY_NAME, PROCESS_STEP, PROCESS_ORDER, PROCESS_STATUS, PROCESS_START_TIME, PROCESS_END_TIME, PROCESS_NOTES, CREATED_BY)
            VALUES (:1, :2, :3, :4, 'SKIPPED', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), :5, CURRENT_USER())`;
        snowflake.createStatement({sqlText: skipSql, binds: [batchRunId, params.entity_name, params.process_step, orderRes.getColumnValue(1), skipReason]}).execute();
        snowflake.createStatement({sqlText: "COMMIT"}).execute();
        return {status: "success", action: "skip"};
        
    } else {
        throw new Error("Invalid action. Allowed: initiate, complete, error, skip");
    }
} catch (err) {
    return {status: "error", error: err.message};
}
$$;


-- =====================================================
-- 4. PRC_DBT_PUBLISH_TO_TARGET (in DBT_CONTROL schema)
-- Merges WRK tables to UDL target tables
-- =====================================================
CREATE OR REPLACE PROCEDURE DBT_CONTROL.PRC_DBT_PUBLISH_TO_TARGET(PARAMS_JSON VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    var params = JSON.parse(PARAMS_JSON);
    if (!params.batch_run_id) throw new Error("Missing: batch_run_id");
    
    var batchRunId = params.batch_run_id;
    var sourceTableType = params.source_table_type || 'employee_newsletter';
    
    var checkSql = "SELECT BATCH_RUN_ID FROM EXECUTION_RUN_STATS.DBT_BATCH_RUN WHERE BATCH_RUN_ID = :1 AND ACTIVE_FLAG = TRUE";
    var checkRes = snowflake.createStatement({sqlText: checkSql, binds: [batchRunId]}).execute();
    if (!checkRes.next()) throw new Error("No active batch found for batch_run_id: " + batchRunId);
    
    var getEntitiesSql = `SELECT ID, WORK_TABLE, TARGET_TABLE, DBT_MODEL_NAME 
        FROM CONFIG.DBT_ENTITY_MAPPING 
        WHERE ACTIVE_FLAG = TRUE AND WORK_TABLE IS NOT NULL AND TARGET_TABLE IS NOT NULL
        ORDER BY ID`;
    var entitiesRes = snowflake.createStatement({sqlText: getEntitiesSql}).execute();
    
    var results = [];
    
    while (entitiesRes.next()) {
        var entityId = entitiesRes.getColumnValue(1);
        var wrkTable = entitiesRes.getColumnValue(2);
        var tgtTable = entitiesRes.getColumnValue(3);
        var modelName = entitiesRes.getColumnValue(4);
        
        var orderSql = "SELECT COALESCE(MAX(PROCESS_ORDER), 0) + 1 FROM EXECUTION_RUN_STATS.DBT_PROCESS_RUN WHERE BATCH_RUN_ID = :1";
        var orderRes = snowflake.createStatement({sqlText: orderSql, binds: [batchRunId]}).execute();
        orderRes.next();
        var processOrder = orderRes.getColumnValue(1);
        
        var initSql = `INSERT INTO EXECUTION_RUN_STATS.DBT_PROCESS_RUN 
            (BATCH_RUN_ID, ENTITY_ID, ENTITY_NAME, PROCESS_STEP, PROCESS_ORDER, PROCESS_STATUS, PROCESS_START_TIME, CREATED_BY)
            VALUES (:1, :2, :3, 'wrk_to_udl', :4, 'RUNNING', CURRENT_TIMESTAMP(), CURRENT_USER())`;
        snowflake.createStatement({sqlText: initSql, binds: [batchRunId, entityId, modelName, processOrder]}).execute();
        
        try {
            var countSql = "SELECT COUNT(*) FROM " + wrkTable;
            var countRes = snowflake.createStatement({sqlText: countSql}).execute();
            countRes.next();
            var wrkCount = countRes.getColumnValue(1);
            
            if (wrkCount === 0) {
                var skipSql = `UPDATE EXECUTION_RUN_STATS.DBT_PROCESS_RUN
                    SET PROCESS_STATUS = 'SKIPPED', PROCESS_END_TIME = CURRENT_TIMESTAMP(),
                        PROCESS_NOTES = 'No data in WRK table', UPDATED_DATE = CURRENT_TIMESTAMP()
                    WHERE BATCH_RUN_ID = :1 AND ENTITY_NAME = :2 AND PROCESS_STEP = 'wrk_to_udl' AND PROCESS_STATUS = 'RUNNING'`;
                snowflake.createStatement({sqlText: skipSql, binds: [batchRunId, modelName]}).execute();
                results.push({entity: modelName, status: 'skipped', rows: 0});
                continue;
            }
            
            var colsSql = `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = UPPER(SPLIT_PART(:1, '.', 1)) AND TABLE_NAME = UPPER(SPLIT_PART(:1, '.', 2)) AND COLUMN_NAME != 'ID'
                ORDER BY ORDINAL_POSITION`;
            var colsRes = snowflake.createStatement({sqlText: colsSql, binds: [wrkTable]}).execute();
            
            var updateCols = [];
            var insertCols = [];
            var insertVals = [];
            var skipUpdateCols = ['CODE', 'TENANT_CODE', 'CREATED_DATETIME', 'CREATED_BY', 'CREATED_BATCH_RUN_ID'];
            
            while (colsRes.next()) {
                var col = colsRes.getColumnValue(1);
                if (skipUpdateCols.indexOf(col) === -1) {
                    updateCols.push(col + ' = src.' + col);
                }
                insertCols.push(col);
                insertVals.push('src.' + col);
            }
            
            var mergeSql = `MERGE INTO ${tgtTable} AS tgt USING ${wrkTable} AS src
                ON tgt.code = src.code AND tgt.tenant_code = src.tenant_code
                WHEN MATCHED THEN UPDATE SET ${updateCols.join(', ')}
                WHEN NOT MATCHED THEN INSERT (${insertCols.join(', ')}) VALUES (${insertVals.join(', ')})`;
            
            snowflake.createStatement({sqlText: mergeSql}).execute();
            
            var completeSql = `UPDATE EXECUTION_RUN_STATS.DBT_PROCESS_RUN
                SET PROCESS_STATUS = 'COMPLETED', PROCESS_END_TIME = CURRENT_TIMESTAMP(),
                    PROCESS_DURATION_SECONDS = DATEDIFF('SECOND', PROCESS_START_TIME, CURRENT_TIMESTAMP()),
                    ROWS_PROCESSED = :1, UPDATED_DATE = CURRENT_TIMESTAMP()
                WHERE BATCH_RUN_ID = :2 AND ENTITY_NAME = :3 AND PROCESS_STEP = 'wrk_to_udl' AND PROCESS_STATUS = 'RUNNING'`;
            snowflake.createStatement({sqlText: completeSql, binds: [wrkCount, batchRunId, modelName]}).execute();
            
            results.push({entity: modelName, status: 'completed', rows: wrkCount});
            
        } catch (mergeErr) {
            var errSql = `UPDATE EXECUTION_RUN_STATS.DBT_PROCESS_RUN
                SET PROCESS_STATUS = 'FAILED', PROCESS_END_TIME = CURRENT_TIMESTAMP(),
                    ERROR_MESSAGE = :1, UPDATED_DATE = CURRENT_TIMESTAMP()
                WHERE BATCH_RUN_ID = :2 AND ENTITY_NAME = :3 AND PROCESS_STEP = 'wrk_to_udl' AND PROCESS_STATUS = 'RUNNING'`;
            snowflake.createStatement({sqlText: errSql, binds: [mergeErr.message.substring(0, 4000), batchRunId, modelName]}).execute();
            results.push({entity: modelName, status: 'failed', error: mergeErr.message});
        }
    }
    
    snowflake.createStatement({sqlText: "COMMIT"}).execute();
    return {status: "success", batch_run_id: batchRunId, results: results};
    
} catch (err) {
    return {status: "error", error: err.message};
}
$$;


-- =====================================================
-- Grant execute to role
-- =====================================================
GRANT USAGE ON PROCEDURE DBT_CONTROL.PRC_DBT_BATCH_HANDLER(VARCHAR) TO ROLE R_DEPLOYMENT_ADMIN_DEV;
GRANT USAGE ON PROCEDURE DBT_CONTROL.PRC_DBT_MANAGE_TRANSIENT_OBJECTS(VARCHAR) TO ROLE R_DEPLOYMENT_ADMIN_DEV;
GRANT USAGE ON PROCEDURE DBT_CONTROL.PRC_DBT_PROCESS_HANDLER(VARCHAR) TO ROLE R_DEPLOYMENT_ADMIN_DEV;
GRANT USAGE ON PROCEDURE DBT_CONTROL.PRC_DBT_PUBLISH_TO_TARGET(VARCHAR) TO ROLE R_DEPLOYMENT_ADMIN_DEV;
