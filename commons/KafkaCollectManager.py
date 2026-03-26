import logging
import os.path
import socket

from typing import List
import pytz
from airflow.models import Variable

from commons.CollectHistory import CollectHistory
from commons.KafkaManager import KafkaManager
from commons.HdfsFileUploader import HdfsFileUploader
from commons.KafkaSend import KafkaSend
from commons.MetaInfoHook import MetaInfoHook, InterfaceInfo
from commons.ProvideManager import ProvideManager

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))


class KafkaCollectManager:
    def __init__(self, context):
        self.dag_id = context['dag'].dag_id
        self.interface_id = self._get_interface_id(context)
        self.task_id = context['task_instance'].task_id
        self.data_interval_end = context['data_interval_end'].replace(tzinfo=pytz.utc).astimezone(pytz.timezone('Asia/Seoul'))
        self.execution_date = context['execution_date']
        self.dag = context['dag']
        self.context = context
        logger.info("data_interval_end: %s, execution_date : %s",self. data_interval_end, self.execution_date)


    def _get_interface_id(self, context):
        result = context['dag'].dag_id

        if context['dag_run'].external_trigger:
            # retry_dag에서 호출된 경우임
            result = context['dag_run'].conf.get('interface_id')

        logger.info('interface_id: %s', result)
        return result


    def start(self):
        formatted_start_time = self.data_interval_end.strftime('%Y%m%d%H%M')
        logger.info("target_time : %s",formatted_start_time)


        kafka_send = None
        meta_info_hook = None
        try:

            # kafka_send = KafkaSend()
            current_host_name = socket.gethostname()

            collect_history = CollectHistory(self.interface_id, self.dag_id, self.task_id, current_host_name, "1", self.execution_date)
            kafka_send = KafkaSend()
            meta_info_hook = MetaInfoHook(kafka_send, collect_history)

            interface_info: InterfaceInfo = meta_info_hook.get_interface_info(self.interface_id)
            source_infos = meta_info_hook.get_source_infos(self.interface_id)

            statusCodeInfo = meta_info_hook.get_status_code()
            kafka_send.set_status_code(statusCodeInfo)

            collect_history.set_statuscode_info(statusCodeInfo)
            collect_history.set_server_info(interface_info)


            interface_info.start_time = formatted_start_time
            interface_info.target_time = formatted_start_time

            logger.info(" interface info : %s", interface_info)
            for source_info in source_infos:
                logger.debug("file : %s", source_info)

            manager = KafkaManager(interface_info, source_infos, kafka_send, collect_history)
            manager.start()
            info_dat_path = manager.info_dat_path
            logger.info("info_dat_path : %s", info_dat_path)
            # infoDatPath = "/data2/20230621/12/IF-T2-7022-01/info.dat"


            if os.path.exists(str(info_dat_path)):
                hdfs_file_uploader = HdfsFileUploader(kafka_send, collect_history, None, info_dat_path, None, stdout=True)
                hdfs_file_uploader.start()

                collect_provide_infos = meta_info_hook.get_collect_provide_infos(self.interface_id)
                # info.dat file needs to upload at here

                ProvideManager(interface_info, collect_provide_infos, self.context).provide_from_ftp(info_dat_path)
            else:
                logger.warning("No info.dat file... no file downloaded")

        finally:
            if kafka_send:
                kafka_send.close()
            if meta_info_hook:
                meta_info_hook.close_db_session()