from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime
import pendulum
from airflow.models import Variable
import logging

############################## Config Section ####################################

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))

local_tz = pendulum.timezone("Asia/Seoul")


"""
{
	"conf":{
		"interface_id":"IF-T-XXXXX",
		"files":{
			"interface_id,history_seq,data_id,base_date,protocol",
			"interface_id,history_seq,data_id,base_date,protocol"
			"interface_id,history_seq,data_id,base_date,protocol"
		}
	}
}
"""

def Set_Info(**context):

    interface = context['dag_run'].conf.get('interface_id')
    files = context['dag_run'].conf.get('files')

    logger.info(f'interface: [{interface}], files: [{files}]')

    if interface != None and files != None :  
        return "re_Run"
    else:
        return "End_Schedule"   
                        
def re_Run(**context):

    interface = context['dag_run'].conf.get('interface_id')
    files = context['dag_run'].conf.get('files')
    dag_run_id = context['dag_run'].run_id
    
    logger.info(f'interface: [{interface}], dag_run_id: [{dag_run_id}], files: [{files}]')
                       
###################################### dag define #############################################

args = {'owner':'seo'}                
with DAG(
        dag_id="reRun_sftp",
        start_date=datetime(2023, 1, 1, tzinfo=local_tz),
        schedule_interval='@once',
        #schedule_interval='00 * * * *',
        catchup=False,
        tags=['TEST'],
        default_args=args
) as dag:
    
    StartSchedule = DummyOperator(task_id="Start_Schedule")    
    SetInfo = BranchPythonOperator(task_id="SetInfo", provide_context=True, python_callable=Set_Info)
    reRun = PythonOperator(task_id="re_Run", provide_context=True, python_callable=re_Run) 
    EndSchedule = DummyOperator(task_id="End_Schedule",trigger_rule='one_success')  

######################################### task sequence ##########################################
    
    StartSchedule >> SetInfo >> [EndSchedule,reRun]
    EndSchedule
    reRun >> EndSchedule
