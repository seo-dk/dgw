import logging
import os.path
import socket
import fnmatch
from typing import List

import pytz
from airflow.models import Variable

from commons.CollectHistory import CollectHistory
from commons.FtpSftpManager import FtpSftpManager, CollectedFileInfo
from commons.HdfsFileUploader import HdfsFileUploader
from commons.KafkaSend import KafkaSend
from commons.MetaInfoHook import MetaInfoHook, InterfaceInfo, SourceInfo
from commons.ProvideManager import ProvideManager
from commons.Util import Util
from commons.RemoveFileManager import RemoveFileManager

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))


class FileCollectManager:
    def __init__(self, context):
        self.dag_id = context['dag'].dag_id
        self.interface_id = self._get_interface_id(context)
        self.task_id = context['task_instance'].task_id
        self.data_interval_end = context['data_interval_end'].replace(tzinfo=pytz.utc).astimezone(pytz.timezone('Asia/Seoul'))
        self.execution_date = context['execution_date']
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
            logger.info("interface info : %s",interface_info)
            collect_history.set_server_info(interface_info)


            interface_info.start_time = formatted_start_time
            ## change time pattern to real time
            Util.replace_time_pattern(source_infos, interface_info)
            Util.parse_noti(interface_info, formatted_start_time)

            logger.debug(" interface : %s", interface_info)
            for source_info in source_infos:
                logger.debug("file : %s", source_info)

            manager = FtpSftpManager(interface_info, source_infos, kafka_send)
            collected_file_infos = manager.start_collect(collect_history)

            info_dat_path = manager.get_info_dat_path()
            logger.info("info_dat_path : %s", info_dat_path)
            # infoDatPath = "/data2/20230621/12/IF-T2-7022-01/info.dat"
            if os.path.exists(str(info_dat_path)):
                hdfs_file_uploader = HdfsFileUploader(kafka_send, collect_history, interface_info.ftp_proto_info.delimiter, info_dat_path, interface_info.ftp_proto_info.encode,
                                                      stdout=True)
                hdfs_file_uploader.start()

                collect_provide_infos = meta_info_hook.get_collect_provide_infos(self.interface_id)
                ProvideManager(interface_info, collect_provide_infos).provide_from_ftp(info_dat_path)
            else:
                logger.warning("No info.dat file... no file downloaded")
            self._remove_file(collected_file_infos)

        finally:
            if kafka_send:
                kafka_send.close()
            if meta_info_hook:
                meta_info_hook.close_db_session()

    def _remove_file(self, collected_file_infos: List[CollectedFileInfo]):
        for collected_file_info in collected_file_infos:
            local_path = collected_file_info.local_path_infos.local_tmp_full_path
            hadoop_path = collected_file_info.partitions.get('hdfsPath')
            file_name = os.path.basename(str(local_path))

            if collected_file_info.source_info.file_proto_info.crypto_rules.enabled and collected_file_info.source_info.file_proto_info.crypto_rules.store_raw_and_crypto:
                staging_local_path = str(local_path).replace("/data/idcube_out", "/data/staging/raw/idcube_out")
                staging_hadoop_path = str(hadoop_path).replace("/idcube_out", "/staging/raw/idcube_out")

                logger.info(f"remove staging_local_path {staging_local_path}")
                RemoveFileManager(staging_local_path, "local").start()
                logger.info(f"remove staging_hadoop_path {staging_hadoop_path}")
                RemoveFileManager(staging_hadoop_path, "hadoop").start()
                logger.info("remove success")

            if collected_file_info.source_info.file_proto_info.delete_after_provide:
                logger.info(f"remove local_path {local_path}")
                RemoveFileManager(collected_file_info.local_path_infos.local_tmp_full_path, "local").start()
                logger.info(f"remove hadoop_path {hadoop_path}")
                RemoveFileManager(hadoop_path, "hadoop").start()
                logger.info("remove success")