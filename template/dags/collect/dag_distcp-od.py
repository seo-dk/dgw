from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.session import provide_session
from airflow.exceptions import AirflowSkipException
from airflow.utils.state import State
from airflow.models import DagRun

from commons.DistcpOdManager import DistcpOdManager
from datetime import datetime, timedelta
import pendulum
from commons.SmsSender import send_sms_alert

def execute_task(**context):
    DistcpOdManager(context).execute()

def check_duplicate_run(**context):
    dag_id = context['dag'].dag_id
    conf = context.get('dag_run').conf or {}    
    
    @provide_session
    def find_existing_run(session, dag_id, conf):    
        return session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.state == State.RUNNING,
            DagRun.conf == conf
        ).count()
    
    count = find_existing_run(dag_id=dag_id, conf=conf)
    if count > 1:
        raise AirflowSkipException("A DAG run with the same parameter is already running.")    

args = {
    'owner': 'mobigen',
    'queue': $IP_NET,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': $EMAIL,
    'retries': $RETRIES,
    'retry_delay': timedelta(minutes = 1),
}

with DAG(
    dag_id=$DAG_ID,
    description=$DESCRIPTION,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1, tzinfo = pendulum.timezone('Asia/Seoul')),
    catchup=False,
    max_active_runs=$MAX_ACTIVE_RUNS,
    max_active_tasks=$MAX_ACTIVE_TASKS,
    default_args=args,
    tags=$TAGS,
    dagrun_timeout=timedelta(hours=24)
) as dag:
    check_duplicate = PythonOperator(task_id='CheckDuplicateRun', provide_context = True, python_callable=check_duplicate_run)
    execute_task_operator = PythonOperator(task_id = 'ExecuteTask', provide_context = True, python_callable = execute_task, on_failure_callback=(None if $IGNORE_SMS_ALERT else send_sms_alert))

    check_duplicate >> execute_task_operator
