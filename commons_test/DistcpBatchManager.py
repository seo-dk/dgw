import logging
import os.path
import socket
import re
import pytz
from airflow.models import Variable
from typing import List

from commons_test.CollectHistory import CollectHistory
from commons_test.KafkaSend import KafkaSend
from commons_test.MetaInfoHook import MetaInfoHook, InterfaceInfo, SourceInfo
from commons_test.Util import Util
from commons_test.DistcpBatchHandler import DistcpBatchHandler
from commons_test.PathUtil import PathUtil
from commons_test.RemoveFileManager import RemoveFileManager

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))


class DistcpBatchManager:
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


    def execute(self):
        kafka_send = None
        meta_info_hook = None
        try:
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

            # 배치 run 기준 시각 (KafkaCollectManager와 동일)
            interface_info.start_time = self.data_interval_end.strftime('%Y%m%d%H%M')
            logger.info("interface_info.start_time : %s", interface_info.start_time)

            distcp_batch_handler = DistcpBatchHandler(interface_info, collect_history, kafka_send)

            hdfs_src_paths = ""
            collected_hdfs_path = []

            for source_info in source_infos:
                source_info: SourceInfo
                logger.debug("file : %s", source_info)
                # base_src_path : /db=p_common/tb=atlas_coverage_5g_bin1m_1w/dt=20241017
                base_src_path, interface_info.target_time = self._get_hdfs_src_path(source_info, interface_info)
                # hdfs_src_path : s3a://s3-idcube-prd-personal/db=p_common/tb=atlas_coverage_5g_bin1m_1w/dt=20241017
                hdfs_src_path = f'{interface_info.distcp_proto_info.source_dir}{base_src_path}'
                # hdfs_dest_path : /idcube_out/db=p_common/tb=atlas_coverage_5g_bin1m_1w/dt=20241017
                hdfs_dest_path, collect_partitions = self._get_hdfs_dest_path(
                    base_src_path, source_info.hdfs_dir, source_info, interface_info
                )
                logger.info("hdfs_src_path: %s, hdfs_dest_path: %s", hdfs_src_path, hdfs_dest_path)

                hdfs_collect_path = distcp_batch_handler.execute(source_info, hdfs_src_path, hdfs_dest_path, collect_partitions)
                collected_hdfs_path.append(hdfs_collect_path)
            logger.info(f"collected file paths : {collected_hdfs_path}")
            # self._remove_file(source_infos, collected_hdfs_path)
        finally:
            if kafka_send:
                logger.info("kafka close")
                kafka_send.close()
            if meta_info_hook:
                meta_info_hook.close_db_session()


    def _get_hdfs_src_path(self, source_info, interface_info: InterfaceInfo):
        # /db=d_psx/tb=dpsx_5g_nsn_ran_ncela_1h/dt=${yyyyMMdd}/hh=${HH-5H}
        src_path = source_info.distcp_proto_info.source_dir
        start_time = interface_info.start_time

        pattern = re.compile(r'\$\{[^}]+\}')
        matches = pattern.findall(src_path)
        replacements = []

        for match in matches:
            replacement = Util.get_calculate_pattern(match, start_time)
            replacements.append(replacement)
            logger.info("match: %s, replacement: %s", match, replacement)
            src_path = src_path.replace(match, replacement)

        # ${} 치환 결과를 순서대로 이어 붙임. placeholder 없으면 배치 시각.
        target_time = "".join(replacements) if replacements else start_time
        return src_path, target_time

    def _get_hdfs_dest_path(self, hdfs_src_path, hdfs_dest_dir, source_info: SourceInfo, interface_info: InterfaceInfo):
        dest_pre_path = '/idcube_out'
        hdfs_dest_path = f'{dest_pre_path}{hdfs_src_path}'

        # DD/HH: dt=YYYYMMDD, hh=HH
        pattern_dt_hh = r"/dt=(\d{8})(?:/hh=(\d{2}))?"
        match_dt_hh = re.search(pattern_dt_hh, hdfs_src_path)
        # MM(월별): ym=YYYYMM
        pattern_ym = r"/ym=(\d{6})"
        match_ym = re.search(pattern_ym, hdfs_src_path)

        collect_partitions = None

        proto_partitions = source_info.distcp_proto_info.partitions if source_info.distcp_proto_info else None
        if proto_partitions:
            proto_segments = PathUtil.get_distcp_batch_partition_segments(source_info, interface_info, interface_info.start_time)
            if proto_segments:
                hdfs_dest_path = f'{dest_pre_path}{hdfs_dest_dir}'
                for item in proto_segments:
                    key = list(item.keys())[0]
                    value = item[key]
                    hdfs_dest_path = f"{hdfs_dest_path.rstrip('/')}/{key}={value}"
                collect_partitions = proto_segments

        if collect_partitions is None:
            if match_dt_hh:
                dt_value = match_dt_hh.group(1)
                hh_value = match_dt_hh.group(2) if match_dt_hh.group(2) else None
                hdfs_dest_path = f'{dest_pre_path}{hdfs_dest_dir}/dt={dt_value}'
                if hh_value:
                    hdfs_dest_path = f'{hdfs_dest_path}/hh={hh_value}'
            elif match_ym:
                ym_value = match_ym.group(1)
                hdfs_dest_path = f'{dest_pre_path}{hdfs_dest_dir}/ym={ym_value}'
            elif source_info.distcp_proto_info.no_partitions:
                hdfs_dest_path = f'{dest_pre_path}{source_info.hdfs_dir}'

        # temp
        if self.interface_id == 'C-IDCUBE-DISTCP-DD-0002' or self.interface_id == 'C-IDCUBE-DISTCP-DD-0003' or self.interface_id == 'C-IDCUBE-DISTCP-DD-0004'\
                or self.interface_id == 'C-IDCUBE-DISTCP-DD-0002-TEST':
            hdfs_dest_path = os.path.join(hdfs_dest_path, "v3")

        return hdfs_dest_path, collect_partitions

    def _remove_file(self, source_infos: List[SourceInfo], hdfs_collected_paths):
        for hdfs_collected_path in hdfs_collected_paths:
            for source_info in source_infos:
                if not source_info.hdfs_dir in hdfs_collected_path:
                    continue
                if source_info.distcp_proto_info.crypto_rules.enabled:
                    if source_info.distcp_proto_info.crypto_rules.is_encrypted:
                        hdfs_collected_path = hdfs_collected_path.replace("/idcube_out", "/staging/raw/idcube_out")
                    logger.info(f"remove {hdfs_collected_path}")
                    RemoveFileManager(hdfs_collected_path, "hadoop").start()
                    logger.info("remove success")
                if source_info.distcp_proto_info.delete_after_provide:
                    hdfs_dest_path = hdfs_collected_path.replace("/staging/raw", "")
                    logger.info(f"remove {hdfs_dest_path}")
                    RemoveFileManager(hdfs_dest_path, "hadoop").start()
                    logger.info("remove success")

