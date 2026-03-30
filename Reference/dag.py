import json
from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utils.commons import get_alert_flag_var, manage_batch_status, extract_full_load_value, update_full_load_variable_on_complete, run_proc_with_query_tag

dag_name = "<DEPLOYMENT_ENV>_data_transformations_employee_newsletter"
dag_schedule_cron = Variable.get("dag_schedule_cron_employee_newsletter_<DEPLOYMENT_ENV>")
mail_ids = Variable.get("notification_mail_ids_<DEPLOYMENT_ENV>")
mail_notification_flag = Variable.get("notification_mail_flag_<DEPLOYMENT_ENV>")
trigger_sisense_data_modal_dag_flag = Variable.get("trigger_sisense_data_modal_dag_flag_<DEPLOYMENT_ENV>")
archive_raw_data_flag = Variable.get("employee_newsletter_archive_raw_data_flag_<DEPLOYMENT_ENV>")
tables_without_clustering = Variable.get("tables_without_clustering_<DEPLOYMENT_ENV>")
data_model_name = Variable.get("data_model_name_<DEPLOYMENT_ENV>")

email_on_success, email_on_failure = get_alert_flag_var(mail_notification_flag.strip().lower())

if len(dag_schedule_cron)==0:
    dag_schedule_cron = None

args = {"owner": "Airflow", "start_date": datetime(2024,4,30), 'email_on_failure': email_on_failure, 'email':mail_ids.split(',')}

dag = DAG(
    dag_id=dag_name, default_args=args, max_active_runs=1, catchup=False, schedule_interval=dag_schedule_cron, tags=["Data Engineering"]
)

batch_name = Variable.get("batch_name_employee_newsletter_<DEPLOYMENT_ENV>").lower()
schema = "udl_batch_process"
json_file_path = "/usr/local/airflow/dags/<DEPLOYMENT_ENV>/variables/<DEPLOYMENT_ENV>_data_transformation_procs.json"
dwh_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn_<DEPLOYMENT_ENV>")
trigger_DS_services_api_flag = Variable.get("trigger_DS_services_api_flag_<DEPLOYMENT_ENV>")
process_running_in_full_load = Variable.get("process_running_in_full_load_<DEPLOYMENT_ENV>")

class LogMessage:
    def __init__(self, transaction_identifier, severity, code, msg):
        self.process_name = "prc_udl_load_orchestrator"
        self.transaction_identifier = transaction_identifier
        self.severity = severity
        self.log_code = code
        self.log_description = msg

class Error:
    def __init__(self, code, msg):
        self.err_code = code
        self.err_msg = msg
        self.process_parameter_name = batch_name


handler_proc_map = {
    1: "prc_udl_batch_handler",
    2: "prc_udl_manage_transient_objects",
    3: "prc_udl_publish_to_target",
    4: "log.prc_logging",
    5: "prc_archive_raw_data"
}

def load_proc_dict(file_path):
    try:
        with open(file_path) as file:
            proc_map = json.load(file)
        return proc_map["EMPLOYEE_NEWSLETTER"]
    except Exception as e:
        raise Exception(f"Exception occurred during load_proc_list: {e}")

def udl_batch_error_handler(action, batch_run_id):
    try:
        dwh_hook.run(sql = f'''call {schema}.{handler_proc_map[1]}('{{"action":"{action}","batch_run_id":"{batch_run_id}"}}')''', autocommit = True)
    except Exception as e:
        raise Exception(f"Exception occurred during udl_batch_error_handler: {e}")       

def log_exception(logs):
    try:
        dwh_hook.run(sql = f'''call {handler_proc_map[4]}('{{"process_name":"{logs.process_name}","severity":"{logs.severity}","log_description":"{logs.log_description}"}}')''', autocommit = True)
    except Exception as e:
        raise Exception(f"Exception occurred during log_exception: {e}")

def call_udl_batch_handler(**context):
    try:
        action = context["action"]
        batch_name = context["batch_name"]
        batch_run_id = 0
        flag = True
        print(batch_name, action)
        if action == "initiate":
            process_running_in_full_load_updated = extract_full_load_value(process_running_in_full_load, "transformations_employee_newsletter", **context)
            prc_response = dwh_hook.get_first(f'''call {schema}.{handler_proc_map[1]}('{{"action":"{action}","batch_name":"{batch_name}","process_running_in_full_load":"{process_running_in_full_load_updated}"}}')''')[0]
            prc_response_dict = json.loads(prc_response)
            print(prc_response_dict)
            status = prc_response_dict["status"]
            if status == "success":
                batch_run_id = prc_response_dict["batch_run_id"]
                context["ti"].xcom_push(key = "batch_run_id", value = batch_run_id)
            else:
                flag = False
        elif action == "complete":
            prc_response = dwh_hook.get_first(f'''call {schema}.{handler_proc_map[1]}('{{"action":"{action}","batch_run_id":"{context["ti"].xcom_pull(task_ids=['task_initiate_process'],key="batch_run_id")[0]}"}}')''')[0]   
            prc_response_dict = json.loads(prc_response)
            print(prc_response_dict)
            status = prc_response_dict["status"]
            if status != "success":
                flag = False
            else:
                update_full_load_variable_on_complete("transformations_employee_newsletter", "<DEPLOYMENT_ENV>", **context)

        if not flag:
            raise Exception(f"Exception occurred during call_udl_batch_handler, STATUS!=SUCCESS")
    except Exception as e:
        err = Error("E0001", str(e))
        log_msg = LogMessage("BatchRunId_"+ str(batch_run_id), "ERROR", err.err_code, err)
        log_exception(log_msg)
        if batch_run_id != 0:
            udl_batch_error_handler("error", batch_run_id)
        raise Exception(f"Exception occurred during call_udl_batch_handler: {e}")    


def call_data_transformation_proc(**context):
    batch_run_id = context["ti"].xcom_pull(task_ids=['task_initiate_process'],key="batch_run_id")[0]
    try:
        run_proc_with_query_tag(
            dwh_hook=dwh_hook,
            schema=schema,
            proc_name=context["proc_name"],
            dag_name=dag_name,
            batch_run_id=batch_run_id,
        )
    except Exception as e:
        err_code = "E000" + str(context["key"]) 
        err = Error(err_code, str(e))
        log_msg = LogMessage("BatchRunId_"+ str(batch_run_id), "ERROR", err.err_code, err)
        log_exception(log_msg)
        if batch_run_id != 0:
            udl_batch_error_handler("error", batch_run_id)    
        raise Exception(f"Exception occurred during call_data_transformation_proc: {e}") 


def manage_transient_objects(**context):
    batch_run_id = context["ti"].xcom_pull(task_ids=['task_initiate_process'],key="batch_run_id")[0]
    try:
        prc_response = dwh_hook.get_first(f'''call {schema}.{handler_proc_map[2]}('{{"batch_run_id":"{batch_run_id}"}}')''')[0]
        prc_response_dict = json.loads(prc_response)
        print(prc_response_dict)
        status = prc_response_dict["status"]
        if status != "success":
            raise Exception(f"Exception occurred during manage_transient_objects, Response != SUCCESS")
    except Exception as e:
        err = Error("E0002", str(e))
        log_msg = LogMessage("BatchRunId_"+ str(batch_run_id), "ERROR", err.err_code, err)
        log_exception(log_msg)
        if batch_run_id != 0:
            udl_batch_error_handler("error", batch_run_id)  
        raise Exception(f"Exception occurred during manage_transient_objects: {e}") 

def publish_to_target(**context):
    batch_run_id = context["ti"].xcom_pull(task_ids=['task_initiate_process'],key="batch_run_id")[0]
    try:
        prc_response = dwh_hook.get_first(f'''call {schema}.{handler_proc_map[3]}('{{"batch_run_id":"{batch_run_id}","nonClusteredTables":{tables_without_clustering}}}')''')[0]
        prc_response_dict = json.loads(prc_response)
        print(prc_response_dict)
        status = prc_response_dict["status"]
        if status != "success":
            raise Exception(f"Exception occurred during publish_to_target, STATUS!=SUCCESS") 
    except Exception as e:
        err = Error("E0003", str(e))
        log_msg = LogMessage("BatchRunId_"+ str(batch_run_id), "ERROR", err.err_code, err)
        log_exception(log_msg)
        if batch_run_id != 0:
            udl_batch_error_handler("error", batch_run_id)  
        raise Exception(f"Exception occurred during publish_to_target: {e}")

def post_transformation_data_archive(**context):
    batch_run_id = context["ti"].xcom_pull(task_ids=['task_initiate_process'],key="batch_run_id")[0]
    try:
        proc_name = context["proc_name"]
        service_name = context["service_name"]
        archive_entity_mapping = context["archive_entity_mapping"]
        dwh_hook.run(sql = f'''call {schema}.{proc_name}('{service_name}','{archive_entity_mapping}')''', autocommit = True)
    except Exception as e:
        err = Error("E0005", str(e))
        log_msg = LogMessage("BatchRunId_"+ str(batch_run_id), "ERROR", err.err_code, err)
        log_exception(log_msg)
        if batch_run_id != 0:
            udl_batch_error_handler("error", batch_run_id)  
        raise Exception(f"Exception occurred during {proc_name}: {e}")

with dag:
    data_transformation_proc_map = load_proc_dict(json_file_path)
    start = EmptyOperator(task_id="Process_Started")
    task_manage_batch_status = PythonOperator(task_id="task_manage_batch_status", python_callable=manage_batch_status, op_kwargs = {"batch_name":batch_name, "dwh_hook": dwh_hook})
    task_manage_batch_status.set_downstream(start)
    end = EmptyOperator(task_id="End_of_Process")
    if len(data_transformation_proc_map)>0:
        task_initiate_process = PythonOperator(task_id="task_initiate_process", python_callable=call_udl_batch_handler, op_kwargs = {"action": "initiate", "batch_name":batch_name})
        task_manage_transient_objects = PythonOperator(task_id="task_manage_transient_objects", python_callable=manage_transient_objects)
        curr_task = ""
        prev_task = ""
        parallel_task_list = []
        dummy_operator_list = []
        for key, proc_list in data_transformation_proc_map.items():
            if len(proc_list) > 1:
                if len(parallel_task_list)>0:
                    curr_task = EmptyOperator(task_id = f'offLoadTask_{key}')
                    curr_task.set_upstream(parallel_task_list)
                    parallel_task_list = []
                    prev_task = curr_task
                for proc in proc_list:
                    curr_task  = f"task_{proc}"
                    curr_task = PythonOperator(task_id=curr_task, python_callable=call_data_transformation_proc, op_kwargs = {"proc_name": proc, "key": key})    
                    if prev_task != "":
                        curr_task.set_upstream(prev_task)
                    else:
                        curr_task.set_upstream(task_manage_transient_objects)
                    parallel_task_list.append(curr_task)

            else:
                curr_task  = f"task_{proc_list[0]}"
                curr_task = PythonOperator(task_id=curr_task, python_callable=call_data_transformation_proc, op_kwargs = {"proc_name": proc_list[0], "key": key})
                if len(parallel_task_list)==0:
                    if prev_task != "":
                        curr_task.set_upstream(prev_task)
                    else:
                        curr_task.set_upstream(task_manage_transient_objects)
                    prev_task = curr_task
                else:
                    curr_task.set_upstream(parallel_task_list)
                    parallel_task_list = []
                    prev_task = curr_task

        task_publish_to_target = PythonOperator(task_id="task_publish_to_target", python_callable=publish_to_target)
        task_complete_process = PythonOperator(task_id="task_complete_process", python_callable=call_udl_batch_handler, op_kwargs = {"action": 'complete', "batch_name":batch_name})
        task_archive_raw_data = PythonOperator(task_id="task_archive_raw_data", python_callable=post_transformation_data_archive, op_kwargs = {"proc_name": handler_proc_map[5],"service_name": "employee_newsletter","archive_entity_mapping": "config.archive_entity_mapping"})

        task_complete_process.set_upstream(task_publish_to_target)
        if len(parallel_task_list)>0:
            task_publish_to_target.set_upstream(parallel_task_list)
        else:
            task_publish_to_target.set_upstream(prev_task)

        end.set_upstream(task_complete_process)
        task_initiate_process.set_upstream(start)
        task_manage_transient_objects.set_upstream(task_initiate_process)
    else:
        end.set_upstream(start)

    if not trigger_sisense_data_modal_dag_flag.lower() in ['false']:
        trigger_sisense_trigger_data_model = TriggerDagRunOperator(
            task_id='trigger_sisense_data_model_<DEPLOYMENT_ENV>',
            trigger_dag_id='<DEPLOYMENT_ENV>_sisense_trigger_data_model',
            conf={'data_model_name': data_model_name}
        )
        trigger_sisense_trigger_data_model.set_upstream(end)

    if not archive_raw_data_flag.lower() in ['false']:
        task_archive_raw_data.set_upstream(end)