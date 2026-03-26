import logging
import socket

from airflow.models import Variable

from commons_test.CollectHistory import CollectHistory
from commons_test.HdfsFileUploader import HdfsFileUploader
from commons_test.KafkaSend import KafkaSend
from commons_test.MetaInfoHook import MetaInfoHook
from commons_test.OdFileManager import OdFileManager
from commons_test.ProvideManager import ProvideManager
from commons_test.Util import Util

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))


class OdCollectManager:

    def execute(self, context, dag_id, task_id):
        dag_run = context['dag_run']

        logger.info("dag_run.conf: %s", dag_run.conf)
        interface_id = dag_run.conf.get('interface_id')
        system_id = dag_run.conf.get('system_id')
        table_nm = dag_run.conf.get('table_nm')
        file_path = dag_run.conf.get('file_path')
        dt = dag_run.conf.get('dt')
        hh = dag_run.conf.get('hh')
        mt = dag_run.conf.get('mt')

        execution_date = context['execution_date']
        formatted_start_time = execution_date.strftime('%Y%m%d%H%M')
        logger.info("execution_date : %s", formatted_start_time)

        self.context = context

        kafka_send = None
        meta_info_hook = None

        try:
            currentHostName = socket.gethostname()
            collectHistory = CollectHistory(interface_id, dag_id, task_id, currentHostName, "1", execution_date)
            kafka_send = KafkaSend()
            meta_info_hook = MetaInfoHook(kafka_send, collectHistory)

            interface_info = meta_info_hook.get_interface_info(interface_id)
            logger.debug("interface_info : %s", interface_info)

            filename = Util.get_clean_filename(file_path)
            source_infos = meta_info_hook.get_source_infos(interface_id, filename, table_nm)
            for source_info in source_infos:
                logger.debug("file: %s", source_info)
            statusCodeInfo = meta_info_hook.get_status_code()

            kafka_send.set_status_code(statusCodeInfo)
            collectHistory.set_server_info(interface_info)
            collectHistory.set_statuscode_info(statusCodeInfo)

            target_time = None
            if hh:
                interface_info.interface_cycle = "HH"
                target_time = dt + hh
            elif dt:
                interface_info.interface_cycle = "DD"
                target_time = dt
            elif mt:
                interface_info.interface_cycle = "MM"
                target_time = mt

            logger.info("Dag target_time : %s", target_time)

            for source_info in source_infos:
                source_info.file_proto_info.target_time = target_time

            interface_info.target_time = target_time
            interface_info.start_time = formatted_start_time
            interface_info.job_schedule_exp = "OD"

            collectHistory.set_server_info(interface_info)

            od_file_manager = OdFileManager(interface_info, source_infos, kafka_send)
            od_file_manager.start(collectHistory, file_path)

            info_dat_path = od_file_manager.getInfoDatPath()
            logger.info("info_dat_path : %s", info_dat_path)
            # infoDatPath = "/data2/20230621/12/IF-T2-7022-01/info.dat"
            if info_dat_path:
                hdfs_file_uploader = HdfsFileUploader(kafka_send, collectHistory, interface_info.ftp_proto_info.delimiter, info_dat_path, interface_info.ftp_proto_info.encode,
                                                      stdout=True)
                hdfs_file_uploader.start()

                collect_provide_infos = meta_info_hook.get_collect_provide_infos(interface_id)
                ProvideManager(interface_info, collect_provide_infos, self.context).provide_from_ftp(info_dat_path)
            else:
                logger.warning("info_dat_path is None, File download failed...")
        finally:
            if kafka_send:
                kafka_send.close()
            if meta_info_hook:
                meta_info_hook.close_db_session()
