from airflow import DAG
from airflow.operators.python import PythonOperator

from commons.FileCollectManager import FileCollectManager
from datetime import datetime, timedelta
import pendulum
from commons.SmsSender import send_sms_alert

def execute_task(**context):
    FileCollectManager(context).start()

args = {
    'owner': 'mobigen',
    'queue': $IP_NET,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': $EMAIL,
    'retries': $RETRIES,
    'retry_delay': timedelta(minutes = 5),
    'retry_exponential_backoff': True
}

with DAG(
    dag_id=$DAG_ID,
    description=$DESCRIPTION,
    schedule_interval=$JOB_SCHEDULE_EXP,
    start_date=datetime(2023, 4, 28, tzinfo = pendulum.timezone('Asia/Seoul')),
    catchup=False,
    max_active_runs=$MAX_ACTIVE_RUNS,
    max_active_tasks=$MAX_ACTIVE_TASKS,
    default_args=args,
    tags=$TAGS,
    dagrun_timeout=timedelta(hours=24)
) as dag:
    execute_task_operator = PythonOperator(task_id = 'ExecuteTask', provide_context = True, python_callable = execute_task, on_failure_callback=(None if $IGNORE_SMS_ALERT else send_sms_alert))
    execute_task_operator
