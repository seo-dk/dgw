import logging
import os
import re
from datetime import datetime, timedelta
from typing import Optional

from airflow.exceptions import AirflowException
from airflow.models import Variable

from commons.DistcpExecutor import DistcpExecutor, DistcpStatus, DistcpConfig
from commons.ProvideMetaInfoHook import ProvideMetaInfoHook, ProvideInterface
from commons.ProvideHistory import ProvideHistory
from commons.ProvideKafkaSend import ProvideKafkaSend
from commons.MetaInfoHook import CollectProvideInfo

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))

class ProvideDistcpManager:
    def __init__(self, context, collect_interface_info=None, provide_file_info: CollectProvideInfo=None, src_path=None, retry=None,
                 provide_interface: Optional[ProvideInterface] = None):
        self.provide_meta_info_hook = ProvideMetaInfoHook()
        self.collect_interface_info = collect_interface_info

        if context:
            self._init_context_vars(context)
        else:
            self._init_collected_vars(provide_file_info, provide_interface, src_path)

        self.retry = False
        if retry:
            self.retry = True

    # trigger
    def _init_context_vars(self, context):
        dag_run = context['dag_run']
        self.interface_id = dag_run.conf.get('interface_id')
        self.src_file_path = dag_run.conf.get('filepath')
        self.src_file_nm = dag_run.conf.get('filenm')
        self.partitions = dag_run.conf.get('partitions')
        self.execution_date = context['execution_date']
        logger.info("dag_run.conf: %s", dag_run.conf)

        self.provide_interface = self.provide_meta_info_hook.get_provide_interface(self.interface_id)
        self.dest_path = self._get_dest_path()

    def _init_collected_vars(self, provide_file_info: CollectProvideInfo, provide_interface: ProvideInterface, src_path):
        if provide_file_info:
            self.interface_id = provide_file_info.provide_interface_id
            self.provide_interface = self.provide_meta_info_hook.get_provide_interface(self.interface_id)
        else:
            self.interface_id = provide_interface.interface_id
            self.provide_interface = provide_interface

        self.src_file_path = src_path
        self.src_file_nm = self._extract_file_nm(self.src_file_path)

        self.partitions = None
        logger.info("interface_id: %s, src_file_path: %s", self.interface_id, self.src_file_path)

        self.dest_path = os.path.join(self._get_dest_path(), self.src_file_nm)


    def execute(self):
        self.distcp_executor = DistcpExecutor()

        if not self._skip_distcp():
            skip_verify_count = 0

            if self.provide_interface.proto_info_json.skip_verify_count:
                skip_verify_count = 1

            clear_path = 0
            if self.provide_interface.proto_info_json.clear_path:
                clear_path = 1

            distcp_config = DistcpConfig(self.src_file_path, self.dest_path, self.interface_id, skip_verify_count, clear_path)
            json_output = self.distcp_executor.exec(distcp_config)

            self._handle_msg_code(json_output)

        self.distcp_executor.close()
        self._send_noti()
        return self.dest_path

    def _extract_partition_value(self, regex_pattern, target_string):
        match = re.search(regex_pattern, target_string)
        if match:
            return match.group(1)
        return None

    def _extract_partitions(self):
        partitions_dict = {}

        if self.partitions:
            for data in self.partitions:
                partitions_dict.update(data)
        else:
            for partition_key, regex in [('dt', r'dt=(\d+)'), ('hh', r'hh=(\d+)'), ('hm', r'hm=(\d+)'), ('nw', r'nw=(\S+)/'), ('sys', r'sys=(\S+)/?')]:
                value = self._extract_partition_value(regex, self.src_file_path)
                if value:
                    if partition_key == 'hh' and len(value) == 2:
                        partitions_dict['hm'] = f'{value}00'
                    partitions_dict[partition_key] = value

        logging.info("Extracted partitions: %s", partitions_dict)
        return partitions_dict

    def _extract_file_nm(self, src_file_path):
        src_file_name = ""

        _, extension = os.path.splitext(src_file_path)

        if extension:
            src_file_name = os.path.basename(src_file_path)
            logger.info("src_file_name: %s", src_file_name)

        return src_file_name

    def _calculate_dt_pattern(self, target_dir, partitions_dict):
        target_dir = target_dir.replace('hdfs://', '')

        pattern = r'(.*)=\$\{.*?\}'
        processed_dir_parts = []

        for part in target_dir.split('/'):
            match = re.search(pattern, part)
            if match:
                logger.info(f'pattern matched in part: "{part}"')
                partition_key = match.group(1)
                if partition_key == "event_time":
                    dt = partitions_dict.get("dt")
                    hh = partitions_dict.get("hh")
                    if dt is None or hh is None:
                        raise Exception('Required keys "dt" or "hh" not found in partitions_dict')
                    partition_value = f'{dt}{hh}'
                else:
                    partition_value = partitions_dict.get(partition_key.replace("etl_", "").replace("cnsl_", "").replace("occr_", "").replace("hr", "hh"))
                    if partition_value is None:
                        raise Exception(f'Partition key {partition_key} not found in partitions_dict')
                processed_dir_parts.append(f'{partition_key}={partition_value}')
            else:
                processed_dir_parts.append(part)

        # test
        processed_target_dir = "hdfs://" + "/".join(processed_dir_parts)
        if "s3a://" in processed_target_dir:
            processed_target_dir = processed_target_dir.replace('hdfs://', '')

        return processed_target_dir

    def _get_dest_path(self):
        partitions_dict = self._extract_partitions()

        logger.info(f'partitions_dict: {partitions_dict}')
        target_dir = self.provide_interface.proto_info_json.target_dir

        if target_dir is None:
            raise Exception("proto_info_json.target_dir is None")

        processed_target_dir = self._calculate_dt_pattern(target_dir, partitions_dict)

        if self.provide_interface.proto_info_json.staging_dir:
            processed_target_dir = self._calculate_dt_pattern(self.provide_interface.proto_info_json.staging_dir, partitions_dict)
        # processed_target_dir = processed_target_dir + "/test"

        logger.info("dest_path: %s", processed_target_dir)

        return processed_target_dir

    def _handle_msg_code(self, json_output):
        msg_code = json_output['code']
        if msg_code != DistcpStatus.SUCCESS.value and msg_code != DistcpStatus.DATA_INEQUALITY.value:
            raise AirflowException(f"Failed to distcp from {self.src_file_path} to {self.dest_path}")

    def _skip_distcp(self):
        if self.provide_interface.proto_info_json.skip_if_partition_exists:
            partitions_dict = self._extract_partitions()
            dt = self._get_pre_src_dt(partitions_dict.get("dt"))
            logger.info("pre_src_dt: %s", dt)

            dest_path = self._get_path_for_skip(self.provide_interface.proto_info_json.target_dir, dt)
            logger.info("dest_path: %s", dest_path)

            if self.distcp_executor.is_path_exist(dest_path):
                return True
            else:
                self.src_file_path = self._get_path_for_skip(self.provide_interface.proto_info_json.local_dir, dt)
                self.dest_path = dest_path
                logger.info("self.src_file_path: %s", self.src_file_path)

        return False

    def _get_pre_src_dt(self, src_dt):
        dt_format = "%Y%m%d"
        formatted_dt = datetime.strptime(src_dt, dt_format)
        dest_dt = formatted_dt - timedelta(days=1)
        str_dest_dt = dest_dt.strftime(dt_format)

        return str_dest_dt

    def _get_path_for_skip(self, full_path, src_dt):
        parent_dir = os.path.dirname(full_path)
        src_file_path = os.path.join(parent_dir, f"dt={src_dt}")

        return src_file_path

    def _send_noti(self):
        provide_history = ProvideHistory(self.collect_interface_info, self.provide_interface, None, self.dest_path, self.retry)
        provide_kafka = ProvideKafkaSend()
        provide_kafka.send_to_kafka(provide_history.provide_history_data)
        provide_kafka.close()


if __name__ == "__main__":
    provide_file_info = None
    src_path = None

    distcp_manager = ProvideDistcpManager(None, provide_file_info, src_path)
    distcp_manager.execute()