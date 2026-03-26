import logging
import socket

import pytz
from airflow.exceptions import AirflowException

from commons.CollectHistory import CollectHistory
from commons.HandleDbConnList import HandleDbConnList
from commons.HdfsFileUploader import HdfsFileUploader
from commons.KafkaSend import KafkaSend
from commons.PathUtil import PathUtil
from commons.ProvideManager import ProvideManager
from commons.MetaInfoHook import MetaInfoHook, SourceInfo
logger = logging.getLogger(__name__)


class DbConnManager:
    def __init__(self, context):
        self.retry = False
        self.source_seq_list = None
        self.collect_file_info_list = None
        self.dag_id = context['dag'].dag_id
        self.interface_id = self._get_interface_id(context)
        self.execution_date = context['execution_date']
        self.task_id = context['task_instance'].task_id
        self.formatted_execution_date = None

        self.data_interval_end = context['data_interval_end'].replace(tzinfo=pytz.utc).astimezone(pytz.timezone('Asia/Seoul'))
        self.formatted_execution_date = self.data_interval_end.strftime('%Y%m%d%H%M')
        logger.info("formatted_execution_date %s", self.formatted_execution_date)
        # logger.info(context)

    def _get_interface_id(self, context):
        result = context['dag'].dag_id

        if context['dag_run'].external_trigger:
            result = context['dag_run'].conf.get('interface_id')

        logger.info('interface_id: %s', result)
        return result

    def start(self):
        kafka_send = None
        meta_info_hook = None
        try:
            kafka_send = KafkaSend()

            current_host_name = socket.gethostname()
            collect_history = CollectHistory(self.interface_id, self.dag_id, self.task_id, current_host_name, "1", self.execution_date)

            meta_info_hook = MetaInfoHook(kafka_send, collect_history)
            interface_info = meta_info_hook.get_interface_info(self.interface_id)
            interface_info.start_time = self.formatted_execution_date

            if self.retry:
                source_infos: list[SourceInfo] = []
                for seq_num in self.source_seq_list:
                    file_info = meta_info_hook.get_dbconn_source_file_info(self.interface_id, seq_num)
                    source_infos.append(file_info)
            else:
                source_infos = meta_info_hook.get_source_infos(self.interface_id)

            status_code_info = meta_info_hook.get_status_code()
            kafka_send.set_status_code(status_code_info)

            collect_history.set_statuscode_info(status_code_info)
            collect_history.set_server_info(interface_info)

            # send retry noti
            if self.retry:
                self.send_retry_noti(collect_history, kafka_send)

            local_info_file_path = PathUtil.get_oracle_local_info_file_path(self.interface_id)
            logger.info("localInfoFilePath: %s", local_info_file_path)

            handle_db_conn_list = HandleDbConnList(self.formatted_execution_date, kafka_send, collect_history, interface_info, source_infos, local_info_file_path,
                                                   self.collect_file_info_list)
            handle_db_conn_list.handle_file_list()

            if local_info_file_path is not None:
                hdfs_file_uploader = HdfsFileUploader(kafka_send, collect_history, '', local_info_file_path, None, stdout=True, retry=self.retry, cleanup_hdfs_dir=True)
                hdfs_file_uploader.start()
            else:
                logger.error("No info.dat path")
                raise AirflowException("No info.dat path")

            # provide logic
            collect_provide_infos = meta_info_hook.get_collect_provide_infos(self.interface_id)
            ProvideManager(interface_info, collect_provide_infos, self.retry).provide_from_ftp(local_info_file_path)

        finally:
            if kafka_send:
                kafka_send.close()
            if meta_info_hook:
                meta_info_hook.close_db_session()

    def setRetryValue(self, source_seq_list, collect_file_info_list):
        logger.info("DbConn_Manager setRetryValue")
        self.retry = True
        self.source_seq_list = source_seq_list
        self.collect_file_info_list = collect_file_info_list

        logger.info("source_seq_list: %s", source_seq_list)
        logger.info("collect_file_info_list: %s", collect_file_info_list)

    def send_retry_noti(self, collect_history, kafka_send):
        collect_history.set_retry_true()

        for collect_hist_id in self.collect_file_info_list.keys():
            ulid = collect_history.create_retry_collect_hist(self.collect_file_info_list[collect_hist_id])
            collect_history_dict = collect_history.chdDict[ulid]
            kafka_send.send_to_kafka(collect_history_dict)
