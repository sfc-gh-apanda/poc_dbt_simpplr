"""
DBT Newsletter Data Transformation DAG
======================================
Executes dbt models via Snowflake Native DBT PROJECT.

DAG Params (for manual trigger):
- full_load: true/false - Full load for all entities (default: false)
- full_load_entities: comma-separated entity names for selective full load (default: "")
- select: dbt model selector (default: newsletter_category)

Examples:
  Full load all:        {"full_load": true}
  Selective full load:  {"full_load_entities": "newsletter_category,newsletter"}
  Specific model:       {"select": "wrk_newsletter_category"}
"""

import json
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


# =============================================================================
# CONFIGURATION
# =============================================================================

DAG_ID = "dbt_newsletter_transformation"
SNOWFLAKE_CONN_ID = "snowflake_conn_dev"
BATCH_NAME = "employee_newsletter_data_load"
DBT_SELECT = "newsletter_category"
DATABASE = "COMMON_TENANT_DEV"

# DAG Params
DEFAULT_PARAMS = {
    "full_load": False,
    "full_load_entities": "",  # Comma-separated: "newsletter_category,newsletter"
    "select": DBT_SELECT,
}


# =============================================================================
# DAG DEFINITION
# =============================================================================

default_args = {
    "owner": "data-engineering",
    "start_date": datetime(2024, 1, 1),
    "retries": 0,  # No retries by default - prevent data corruption
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="0 */4 * * *",
    max_active_runs=1,
    catchup=False,
    tags=["dbt", "newsletter", "snowflake-native"],
    params=DEFAULT_PARAMS,
)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_hook():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    return hook


def execute_sql(sql):
    """Execute SQL with AUTOCOMMIT enabled."""
    hook = get_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("ALTER SESSION SET AUTOCOMMIT = TRUE")
    cursor.execute(sql)
    result = cursor.fetchone()
    cursor.close()
    return result


def call_procedure(proc_name, params_dict):
    """Call Snowflake procedure and return parsed response."""
    sql = f"CALL {DATABASE}.DBT_CONTROL.{proc_name}('{json.dumps(params_dict)}')"
    logging.info(f"Executing: {sql}")
    
    result = execute_sql(sql)
    response = result[0] if result else {}
    if isinstance(response, str):
        response = json.loads(response)
    
    logging.info(f"Response: {response}")
    
    if response.get("status") != "success":
        raise Exception(f"{proc_name} failed: {response.get('error')}")
    
    return response


def mark_batch_error(batch_run_id, error_msg):
    """Mark batch as failed."""
    if not batch_run_id:
        return
    try:
        call_procedure("PRC_DBT_BATCH_HANDLER", {
            "action": "error",
            "batch_run_id": batch_run_id,
            "error_message": str(error_msg)[:4000]
        })
    except Exception as e:
        logging.error(f"Failed to mark batch as error: {e}")


# =============================================================================
# TASK FUNCTIONS
# =============================================================================

def check_no_active_batch(**context):
    """Pre-check: Ensure no active batch is running."""
    result = execute_sql(
        f"SELECT COUNT(*) FROM {DATABASE}.DBT_EXECUTION_RUN_STATS.DBT_BATCH_RUN WHERE ACTIVE_FLAG = TRUE"
    )
    if result and result[0] > 0:
        raise Exception("Another batch is already running. Wait or mark it complete/error first.")
    logging.info("No active batch found")


def initiate_batch(**context):
    """Step 1: Initialize batch run."""
    params = context.get("params", {})
    full_load = params.get("full_load", False)
    full_load_entities = params.get("full_load_entities", "")
    
    response = call_procedure("PRC_DBT_BATCH_HANDLER", {
        "action": "initiate",
        "batch_name": BATCH_NAME,
        "full_load": full_load,
        "process_running_in_full_load": full_load_entities,
    })
    
    batch_run_id = response["batch_run_id"]
    context["ti"].xcom_push(key="batch_run_id", value=batch_run_id)
    
    logging.info(f"Batch {batch_run_id} initiated")
    logging.info(f"  full_load: {response.get('full_load')}")
    logging.info(f"  full_load_entities: {full_load_entities or 'none'}")
    
    return batch_run_id


def create_transient_objects(**context):
    """Step 2: Create transient STG and WRK tables."""
    batch_run_id = context["ti"].xcom_pull(task_ids="initiate_batch", key="batch_run_id")
    
    try:
        response = call_procedure("PRC_DBT_MANAGE_TRANSIENT_OBJECTS", {
            "batch_run_id": batch_run_id
        })
        logging.info(f"Created {response.get('tables_created')} transient tables")
    except Exception as e:
        mark_batch_error(batch_run_id, str(e))
        raise


def run_dbt_models(**context):
    """
    Step 3: Execute dbt models via Snowflake Native DBT PROJECT.
    
    On failure:
    - Exception is raised
    - Airflow retries based on default_args (retries=2)
    - If all retries fail, batch is marked as error
    """
    batch_run_id = context["ti"].xcom_pull(task_ids="initiate_batch", key="batch_run_id")
    params = context.get("params", {})
    dbt_select = params.get("select", DBT_SELECT)
    
    try:
        response = call_procedure("PRC_EXECUTE_DBT", {
            "command": "run",
            "select": dbt_select
        })
        logging.info(f"dbt run completed for: {dbt_select}")
        
    except Exception as e:
        # Check if this is the last retry
        try_number = context.get("ti").try_number
        max_tries = context.get("ti").max_tries or 3
        
        logging.error(f"dbt run failed (attempt {try_number}/{max_tries}): {e}")
        
        if try_number >= max_tries:
            # Last retry failed - mark batch as error
            mark_batch_error(batch_run_id, f"dbt run failed after {max_tries} attempts: {str(e)}")
        
        raise  # Re-raise for Airflow to handle retry


def run_dbt_tests(**context):
    """
    Step 4: Execute dbt tests (data quality gate).
    
    On failure:
    - Tests failed = data quality issue
    - Batch is marked as error
    - Pipeline stops before publish
    """
    batch_run_id = context["ti"].xcom_pull(task_ids="initiate_batch", key="batch_run_id")
    params = context.get("params", {})
    dbt_select = params.get("select", DBT_SELECT)
    
    try:
        response = call_procedure("PRC_EXECUTE_DBT", {
            "command": "test",
            "select": dbt_select
        })
        logging.info(f"dbt tests passed for: {dbt_select}")
        
    except Exception as e:
        logging.error(f"dbt tests failed: {e}")
        mark_batch_error(batch_run_id, f"Data quality tests failed: {str(e)}")
        raise


def publish_to_target(**context):
    """Step 5: Publish WRK to UDL target."""
    batch_run_id = context["ti"].xcom_pull(task_ids="initiate_batch", key="batch_run_id")
    
    try:
        response = call_procedure("PRC_DBT_PUBLISH_TO_TARGET", {
            "batch_run_id": batch_run_id
        })
        
        for r in response.get("results", []):
            status = r.get('status')
            entity = r.get('entity')
            if status == 'completed':
                logging.info(f"  ✓ {entity}: deleted={r.get('deleted', 0)}, inserted={r.get('inserted', 0)}")
            else:
                logging.info(f"  ○ {entity}: {status}")
                
    except Exception as e:
        mark_batch_error(batch_run_id, str(e))
        raise


def complete_batch(**context):
    """Step 6: Mark batch as completed."""
    batch_run_id = context["ti"].xcom_pull(task_ids="initiate_batch", key="batch_run_id")
    
    try:
        response = call_procedure("PRC_DBT_BATCH_HANDLER", {
            "action": "complete",
            "batch_run_id": batch_run_id
        })
        logging.info(f"Batch {batch_run_id} completed: {response.get('final_status')}")
        
    except Exception as e:
        mark_batch_error(batch_run_id, str(e))
        raise


# =============================================================================
# DAG TASKS
# =============================================================================

with dag:
    
    start = EmptyOperator(task_id="start")
    
    t_check = PythonOperator(
        task_id="check_no_active_batch",
        python_callable=check_no_active_batch,
    )
    
    t_initiate = PythonOperator(
        task_id="initiate_batch",
        python_callable=initiate_batch,
    )
    
    t_transient = PythonOperator(
        task_id="create_transient_objects",
        python_callable=create_transient_objects,
    )
    
    t_dbt_run = PythonOperator(
        task_id="run_dbt_models",
        python_callable=run_dbt_models,
        # ONLY this task retries - dbt is idempotent
        retries=2,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=10),
    )
    
    t_dbt_test = PythonOperator(
        task_id="run_dbt_tests",
        python_callable=run_dbt_tests,
        # No retries - data quality issue should stop pipeline
    )
    
    t_publish = PythonOperator(
        task_id="publish_to_target",
        python_callable=publish_to_target,
    )
    
    t_complete = PythonOperator(
        task_id="complete_batch",
        python_callable=complete_batch,
    )
    
    end = EmptyOperator(task_id="end")
    
    # Pipeline
    (
        start 
        >> t_check 
        >> t_initiate 
        >> t_transient 
        >> t_dbt_run 
        >> t_dbt_test 
        >> t_publish 
        >> t_complete 
        >> end
    )
