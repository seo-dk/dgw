import logging
import os
import re
import socket
from datetime import datetime
from pathlib import Path
from typing import List

from airflow.exceptions import AirflowException
from airflow.models import Variable

from commons.CollectHistory import CollectHistory
from commons.DistcpExecutor import DistcpExecutor, DistcpStatus, DistcpConfig
from commons.KafkaSend import KafkaSend
from commons.MetaInfoHook import MetaInfoHook, SourceInfo
from commons.ProvideManager import ProvideManager
from commons.ParquetValidator import ParquetValidator
from commons.DistcpOdFileHandler import DistcpOdFileHandler
from commons.RemoveFileManager import RemoveFileManager

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))

class DistcpOdManager:

    def __init__(self, context):
        self._init_context_vars(context)

        currentHostName = socket.gethostname()
        self.collectHistory = CollectHistory(self.interface_id, self.dag_id, self.task_id, currentHostName, "1",
                                             self.execution_date)

        self.kafka_send = KafkaSend()

        self.meta_info_hook = MetaInfoHook(self.kafka_send, self.collectHistory)
        self.interface_info = self.meta_info_hook.get_interface_info(self.interface_id)
        self.source_infos = self.meta_info_hook.get_source_infos(self.interface_id, None, None, None, self.dest_path)
        self.statusCodeInfo = self.meta_info_hook.get_status_code()

        self.kafka_send.set_status_code(self.statusCodeInfo)

        self.collectHistory.set_server_info(self.interface_info)
        self.collectHistory.server_info.period = self._get_period(self.src_path)
        self.collectHistory.set_statuscode_info(self.statusCodeInfo)

        self._prepare_paths(context)

    def _init_context_vars(self, context):
        def _get_interface_id(context):
            result = context['dag'].dag_id

            if context['dag_run'].external_trigger:
                # trigger where retry_dag
                result = context['dag_run'].conf.get('interface_id')

            logger.info('interface_id: %s', result)
            return result

        dag_run = context['dag_run']
        self.dag_id = context['dag'].dag_id
        self.interface_id = _get_interface_id(context)
        self.task_id = context['task_instance'].task_id
        self.src_path = dag_run.conf.get('source_path')
        self.dest_path = dag_run.conf.get('destination_path')
        self.file_name = dag_run.conf.get('filenm')
        self.partitions = dag_run.conf.get('partitions')
        self.execution_date = context['execution_date']
        self.version = dag_run.conf.get('version')
        self.filepath = dag_run.conf.get('filepath')

        self.src_partitions = dag_run.conf.get("src_partitions")
        if self.src_partitions:
            self.interface_id = context['dag'].dag_id
            if context['dag_run'].external_trigger:
                self.interface_id = context['dag_run'].conf.get('interfaceId')
            self.src_path = dag_run.conf.get("sourceDir")
            self.dest_path = dag_run.conf.get('targetDir')
        if not self.file_name:
            self.file_name = dag_run.conf.get('file_name')

        logger.info("dag_run.conf: %s", dag_run.conf)

    def _get_period(self, src_path):
        path_period_mapping = {
            'period=1h': "0 * * * *",
            'period=1d': "0 0 * * *",
            'period=1w': "0 0 * * 4",
            'period=1m': "0 0 1-7 * 5"
        }
        if self.interface_info.job_schedule_exp:
            return self.interface_info.job_schedule_exp
        else:
            return next((value for key, value in path_period_mapping.items() if key in src_path), "")

    def _prepare_paths(self, context):
        dag_run = context['dag_run']
        self.partitions_list = []
        self.partitions_src_str = ""
        self.partitions_dest_str = ""
        self.target_dt = ""

        def _set_partition_by_date():
            for key, value in self.partitions.items():
                if key and value:
                    formatted_value = value
                    dest_value = value

                    if key == "ym" and isinstance(value, str) and len(value) == 6 and value.isdigit():
                        dest_value += "01"

                    self.partitions_list.append({key.replace("ym", "dt"): dest_value})

                    if key in ["dt", "hh", "ym", "hm"]:
                        self.target_dt += str(dest_value)

                    self.partitions_src_str += f"/{key}={formatted_value}"
                    self.partitions_dest_str += f"/{key}={dest_value}"

        def _set_partition_by_src():
            dt = dag_run.conf.get("dt")
            hh = dag_run.conf.get("hh")
            self.target_dt = str(dt+hh)
            for partition_dict in self.src_partitions:
                for key, value in partition_dict.items():
                    self.partitions_list.append({key: value})
                    self.partitions_src_str += f"/{key}={value}"
            if dt:
                self.partitions_dest_str += f"/dt={dt}"
            if hh:
                self.partitions_dest_str += f"/hh={hh}"

        if self.src_partitions:
            _set_partition_by_src()
        else:
            _set_partition_by_date()

        if self.version == "2":
            logger.info('version 2')
            self.src_path = self.filepath.replace("s3:", "s3a:")
            if self.file_name:
                self.src_path += f"/{self.file_name}"
            self.hdfs_source_path = self.src_path
        else:
            self.src_path += self.partitions_src_str
            if self.file_name:
                self.src_path += f"/{self.file_name}"
            self.hdfs_source_path = f'{self.interface_info.distcp_proto_info.source_dir}{self.src_path}'

        logger.info('file_name: %s, src_path: %s, hdfs_source_path: %s', self.file_name, self.src_path, self.hdfs_source_path)

    def execute(self):
        try:
            self._distcp()
        except Exception as e:
            self.kafka_send.sendErrorKafka(self.collectHistory, 7, True, str(e)[:1024])
            raise
        finally:
            self.kafka_send.close()
            self.meta_info_hook.close_db_session()

    def _distcp(self):
        # with DistcpExecutor() as distcp_executor, CheckParquetExecutor() as parquet_executor:
        with DistcpExecutor() as distcp_executor, ParquetValidator() as parquet_validator:
            self.hdfs_dest_path = None
            for source_info in self.source_infos:
                self.hdfs_dest_path = self._get_hdfs_dest_path(source_info)
                coll_hist_id, coll_hist_dict = self._initiate_file_transfer(source_info)

                skip_verify_count = 0
                if source_info.distcp_proto_info.skip_verify_count:
                    skip_verify_count = 1

                clear_path = 0
                if source_info.distcp_proto_info.clear_path:
                    clear_path = 1

                distcp_config = DistcpConfig(self.hdfs_source_path, "hdfs://dataGWCluster" + self.hdfs_dest_path, self.interface_id, skip_verify_count, clear_path)
                json_output = distcp_executor.exec(distcp_config)
                logger.info("json_output : %s", json_output)

                is_parquet = False
                if source_info.distcp_proto_info.convert_to == "parquet":
                    is_parquet = True
                if source_info.distcp_proto_info.format == 'parquet':
                    is_parquet = parquet_validator.validate_parquet("hdfs://dataGWCluster" + self.hdfs_dest_path, self.interface_id)

                logger.info("is_parquet: %s", is_parquet)
                is_collected = self._handle_command(source_info, coll_hist_dict, coll_hist_id, json_output, is_parquet)

                if is_collected:
                    distcp_op_file_handler = DistcpOdFileHandler(self.interface_info, source_info, self.kafka_send, coll_hist_dict, self.hdfs_dest_path)
                    distcp_op_file_handler.start()

                    provide_manager = ProvideManager(self.interface_info, None, False)
                    provide_manager.provide_from_distcp(source_info, self.hdfs_dest_path, self.version, source_info.distcp_proto_info.clear_path)
                else:
                    logger.info("distcp collect failed...")
            if self.hdfs_dest_path:
                self._remove_file(self.hdfs_dest_path, self.source_infos)

    def _get_hdfs_dest_path(self, source_info: SourceInfo):
        self.partitions_dest_str = self.partitions_dest_str.replace('/ym=', '/dt=')

        if self.interface_info.distcp_proto_info.rename_table:
            logger.info("db and table name renamed to hdfs_dir of collect_source")

            dest_pre_path = '/idcube_out'

            if self.version == "2":
                _hdfs_dest_path = os.path.join(dest_pre_path, source_info.hdfs_dir.strip('/'), "v2", self.partitions_dest_str.strip('/'))
            else:
                _hdfs_dest_path = str(dest_pre_path / Path(source_info.hdfs_dir.strip('/')) / Path(self.partitions_dest_str.strip('/')))
        else:
            if self.version == "2":
                _hdfs_dest_path = os.path.join(self.dest_path, "v2", self.partitions_dest_str.strip('/'))
            else:
                _hdfs_dest_path = str(self.dest_path / Path(self.partitions_dest_str.strip('/')))

        if source_info.distcp_proto_info.convert_to == "parquet":
            _hdfs_dest_path = "/staging" + _hdfs_dest_path
        if source_info.distcp_proto_info.crypto_rules.enabled and not source_info.distcp_proto_info.crypto_rules.is_encrypted:
            _hdfs_dest_path = "/staging/raw" + _hdfs_dest_path

        if self.file_name:
            _hdfs_dest_path += f"/{self.file_name}"

        logger.info("target_dt : %s, src_path : %s, hdfs_dest_path : %s", self.target_dt, self.src_path,
                    _hdfs_dest_path)

        return _hdfs_dest_path

    def _initiate_file_transfer(self, source_info):

        collect_hist_id = self.collectHistory.start_collect_h(
            collect_source_seq=source_info.collect_source_seq,
            local_path=self.hdfs_source_path,
            dest_path=self.hdfs_dest_path,
            filename="",
            target_dt=self.target_dt,
            partitions=self.partitions_list
        )

        coll_hist_dict = self.collectHistory.chdDict[collect_hist_id]
        self.collectHistory.set_status_code(coll_hist_dict, 0)
        self.kafka_send.send_to_kafka(coll_hist_dict)

        return collect_hist_id, coll_hist_dict

    def _handle_command(self, source_info, col_hist_dict, collect_hist_id, json_output, is_parquet=False):
        current_time = datetime.now().isoformat()
        col_hist_dict.ended_at = current_time
        col_hist_dict.version = str(2 if is_parquet else 1)

        self.collectHistory.set_partition_info(collect_hist_id, '', json_output.get('dest_size', -1), current_time, self.hdfs_source_path)
        return self._handle_msg_code(source_info, json_output, col_hist_dict)

    def _handle_msg_code(self, source_info: SourceInfo, json_output, coll_hist_dict):
        self.collectHistory.set_status_code(coll_hist_dict, json_output['code'])

        if source_info.distcp_proto_info.convert_to != "parquet" and not source_info.distcp_proto_info.crypto_rules.enabled:
            logger.info(f'col_hist_dict: {coll_hist_dict}')
            self.kafka_send.send_to_kafka(coll_hist_dict)

        msg_code = json_output['code']
        if msg_code == DistcpStatus.SUCCESS.value:
            return True
        else:
            if self.version == "2":
                logger.warning(f"at version 2, failed to distcp from {self.hdfs_source_path} to {self.hdfs_dest_path}, break task")
                return False

            if msg_code != DistcpStatus.DATA_INEQUALITY.value:
                raise AirflowException(f"Failed to distcp from {self.hdfs_source_path} to {self.hdfs_dest_path}")

    def _remove_file(self, hdfs_dest_path, source_infos: List[SourceInfo]):
        for source_info in source_infos:
            if not source_info.hdfs_dir in hdfs_dest_path:
                continue
            if source_info.distcp_proto_info.crypto_rules.enabled:
                if source_info.distcp_proto_info.crypto_rules.is_encrypted:
                    hdfs_dest_path = hdfs_dest_path.replace("/idcube_out","/staging/raw/idcube_out")
                logger.info(f"remove {hdfs_dest_path}")
                RemoveFileManager(hdfs_dest_path, "hadoop").start()
                logger.info("remove success")
            if source_info.distcp_proto_info.delete_after_provide:
                hdfs_dest_path = hdfs_dest_path.replace("/staging/raw", "")
                logger.info(f"remove {hdfs_dest_path}")
                RemoveFileManager(hdfs_dest_path, "hadoop").start()
                logger.info("remove success")