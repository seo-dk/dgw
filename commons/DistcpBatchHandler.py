import logging

from datetime import datetime

import re

from airflow.exceptions import AirflowException
from airflow.models import Variable

from commons.DistcpExecutor import DistcpExecutor, DistcpStatus, DistcpConfig
from commons.MetaInfoHook import SourceInfo
from commons.ProvideMetaInfoHook import ProvideMetaInfoHook
from commons.ProvideManager import ProvideManager
from commons.MetaInfoHook import InterfaceInfo
from commons.ConvertToCsv import ConvertToCsv
from commons.CollectHistory import CollectHistory
from commons.ParquetValidator import ParquetValidator
from commons.DistcpParquetHandler import DistcpParquetHandler
from commons.DistcpBatchFileHandler import DistcpBatchFileHandler

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))


class DistcpBatchHandler:
    def __init__(self, interface_info: InterfaceInfo, collect_history: CollectHistory, kafka_send):
        self.interface_info = interface_info
        self.collect_history = collect_history
        self.kafka_send = kafka_send
        self.provide_manager = ProvideManager(self.interface_info, None, False)

    def execute(self, source_info: SourceInfo, hdfs_src_path, hdfs_dest_path, partitions_list=None):
        try:
            return self._distcp(source_info, hdfs_src_path, hdfs_dest_path, partitions_list)
        except Exception as e:
            self.kafka_send.sendErrorKafka(self.collect_history, 7, True, str(e)[:1024])

            if self.interface_info.distcp_proto_info.skip_failure:
                logger.error("failed distcp %s to %s", hdfs_src_path, hdfs_dest_path)
            else:
                raise

    def _distcp(self, source_info: SourceInfo, hdfs_src_path, hdfs_dest_path, partitions_list=None):
        with DistcpExecutor() as executor, ParquetValidator() as parquet_validator:
            hdfs_dest_path = self._get_hdfs_dest_path(source_info, hdfs_dest_path)

            # DistCP batch에서 manager가 전달하는 proto partitions가 있으면 그대로 사용.
            # 없으면 목적지 경로에서 dt/hh/ym 파싱.
            if partitions_list is None:
                partitions_list = self._get_partitions(hdfs_dest_path)
            target_dt = self._get_target_dt(partitions_list)

            coll_hist_id, coll_hist_dict = self._initiate_file_transfer(source_info, hdfs_src_path, hdfs_dest_path, target_dt, partitions_list)

            skip_verify_count = 0

            clear_path = 0
            clear_subdirs = 0

            if source_info.distcp_proto_info.clear_path:
                clear_path = 1
            if source_info.distcp_proto_info.clear_subdirs:
                clear_subdirs = 1

            distcp_config = DistcpConfig(hdfs_src_path, "hdfs://dataGWCluster" + hdfs_dest_path, source_info.interface_id, skip_verify_count, clear_path, clear_subdirs)
            json_output = executor.exec(distcp_config)

            if source_info.distcp_proto_info.file_rename:
                logger.info("source_info.distcp_proto_info.file_nm: %s", source_info.distcp_proto_info.file_nm)
                executor.exec_rename_file(hdfs_dest_path, source_info.distcp_proto_info.file_nm, "zstd.parquet")

            is_parquet = False
            if source_info.distcp_proto_info.convert_to == "parquet":
                is_parquet = True
            if source_info.distcp_proto_info.format == 'parquet':
                is_parquet = parquet_validator.validate_parquet("hdfs://dataGWCluster" + hdfs_dest_path, source_info.interface_id)

            logger.info("is_parquet: %s", is_parquet)
            is_collected = self._handle_command(hdfs_src_path, hdfs_dest_path, source_info, coll_hist_dict, coll_hist_id, json_output, is_parquet)

            if is_collected:
                distcp_file_handler = DistcpBatchFileHandler(source_info, self.interface_info, hdfs_dest_path, coll_hist_dict, self.kafka_send)
                distcp_file_handler.start()
                self._start_provide(source_info, hdfs_dest_path)
            else:
                logger.info("distcp collect failed...")
            return hdfs_dest_path

    def _initiate_file_transfer(self, source_info, hdfs_source_path, hdfs_dest_path, target_dt, partitions_list):
        collect_hist_id = self.collect_history.start_collect_h(
            collect_source_seq=source_info.collect_source_seq,
            local_path=hdfs_source_path,
            dest_path=hdfs_dest_path,
            filename="",
            target_dt=target_dt,
            partitions=partitions_list
        )

        coll_hist_dict = self.collect_history.chdDict[collect_hist_id]
        self.collect_history.set_status_code(coll_hist_dict, 0)
        self.kafka_send.send_to_kafka(coll_hist_dict)

        return collect_hist_id, coll_hist_dict

    def _handle_command(self, hdfs_src_path, hdfs_dest_path, source_info: SourceInfo, col_hist_dict, collect_hist_id, json_output, is_parquet=False):
        current_time = datetime.now().isoformat()
        col_hist_dict.ended_at = current_time
        col_hist_dict.version = str(2 if is_parquet else 1)

        self.collect_history.set_partition_info(collect_hist_id, '', json_output.get('dest_size', -1), current_time, hdfs_src_path)
        return self._handle_msg_code(hdfs_src_path, hdfs_dest_path, source_info, json_output, col_hist_dict)

    def _handle_msg_code(self, hdfs_src_path, hdfs_dest_path, source_info: SourceInfo, json_output, coll_hist_dict):
        self.collect_history.set_status_code(coll_hist_dict, json_output['code'])

        if source_info.distcp_proto_info.convert_to != "parquet" and not source_info.distcp_proto_info.crypto_rules.enabled:
            logger.info(f'col_hist_dict: {coll_hist_dict}')
            self._send_kafka_message(coll_hist_dict)

        msg_code = json_output['code']
        if msg_code == DistcpStatus.SUCCESS.value:
            return True
        else:
            if msg_code != DistcpStatus.DATA_INEQUALITY.value:
                if not self.interface_info.distcp_proto_info.skip_failure:
                    raise AirflowException(f"Failed to distcp from {hdfs_src_path}")
                else:
                    raise Exception

            return False


    def _get_dest_dict(self, dest_path):
        dest_path_dict = {}
        pattern = re.compile(r'db=([^/]+)/tb=([^/]+)')
        match = pattern.search(dest_path)

        if match:
            dest_path_dict["db"] = match.group(1).lower()
            dest_path_dict["tb"] = match.group(2).lower()
            logger.info("dest_dict[db]: %s, dest_dict[tb]: %s", dest_path_dict["db"], dest_path_dict["tb"])

        return dest_path_dict

    def _get_partitions(self, hdfs_dest_path):
        partitions_list = []

        match_dt_hh = re.search(r"/dt=(\d+)(?:/hh=(\d+))?", hdfs_dest_path)
        match_ym = re.search(r"/ym=(\d{6})", hdfs_dest_path)

        if match_dt_hh:
            dt, hh = match_dt_hh.groups()
            if hh:
                partitions_list.append({'dt': dt})
                partitions_list.append({'hh': hh})
            else:
                partitions_list.append({'dt': dt})
        elif match_ym:
            partitions_list.append({'ym': match_ym.group(1)})
        else:
            logger.info("Input path does not contain the expected dt/hh or ym structure")
            return

        logger.info("partitions_list: %s", partitions_list)

        return partitions_list

    def _get_target_dt(self, partitions_list):
        target_dt = ""

        if partitions_list:
            for dict in partitions_list:
                for value in dict.values():
                    target_dt += value

        logger.info("target_dt: %s", target_dt)
        return target_dt

    def _send_kafka_message(self, coll_hist_dict):
        current_time = datetime.now().isoformat()
        coll_hist_dict.ended_at = current_time
        logger.info(f'col_hist_dict: {coll_hist_dict}')

        self.kafka_send.send_to_kafka(coll_hist_dict)

    def _get_hdfs_dest_path(self, source_info: SourceInfo, hdfs_dest_path):
        if source_info.distcp_proto_info.convert_to == "parquet":
            hdfs_dest_path = "/staging" + hdfs_dest_path
        if source_info.distcp_proto_info.crypto_rules.enabled and not source_info.distcp_proto_info.crypto_rules.is_encrypted:
            hdfs_dest_path = "/staging/raw"+hdfs_dest_path
        return hdfs_dest_path

    def _start_provide(self, source_info, hdfs_dest_path):
        dest_path_dict = self._get_dest_dict(hdfs_dest_path)
        provide_interfaces = ProvideMetaInfoHook().get_provide_interface_by_tb(dest_path_dict.get('db'), dest_path_dict.get('tb'))

        for provide_interface in provide_interfaces:
            version = "1"

            if provide_interface.protocol_cd == 'FTP' or provide_interface.protocol_cd == 'SFTP':
                if (self.interface_info.interface_id == 'C-IDCUBE-DISTCP-DD-0002' or self.interface_info.interface_id == 'C-IDCUBE-DISTCP-DD-0003'
                        or self.interface_info.interface_id == 'C-IDCUBE-DISTCP-DD-0004'):
                    logger.info("version 3")
                    version = "3"

            self.provide_manager.provide_from_distcp_batch(source_info, provide_interface, hdfs_dest_path, version)


