import logging
import socket
from airflow.exceptions import AirflowException
from commons_test.CollectHistory import CollectHistory
from commons_test.HandleDbConnList import HandleDbConnList
from commons_test.HdfsCopy import HdfsCopy
from commons_test.KafkaSend import KafkaSend
from commons_test.MetaInfoHook import MetaInfoHook
from commons_test.SetPath import SetPath
import pytz

logger = logging.getLogger(__name__)

class FTP_Provider_Manager:
    def __init__(self, context):
        self._init_context_vars(context)

    def _init_context_vars(self, context):
        dag_run = context['dag_run']
        self.interface_id = dag_run.conf.get('interface_id')
        self.file_path = dag_run.conf.get('filepath')
        self.file_nm = dag_run.conf.get('filenm')
        self.execution_date = context['execution_date']

        logger.info("dag_run.conf: %s", dag_run.conf)

    def execute(self):
        pass

