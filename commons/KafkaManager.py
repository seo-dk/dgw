import logging
import os.path
import re
from kafka import KafkaConsumer, TopicPartition
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.exceptions import AirflowException
from typing import List
import ulid
from pathlib import Path

from commons.MetaInfoHook import InterfaceInfo, SourceInfo
from commons.CollectHistory import CollectHistory
from commons.KafkaConsume import KafkaMessageConsumer
from commons.ConvertToParquet2_v2 import convert
from commons.PathUtil import PathUtil
from commons.InfodatManager import InfoDatManager


logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))

class KafkaManager:
    def __init__(self, interface_info: InterfaceInfo, source_infos: List[SourceInfo], kafka_send, collect_history: CollectHistory):
        self.interface_info = interface_info
        self.source_infos = source_infos
        self.kafka_send = kafka_send
        self.collect_history = collect_history
        self.info_dat_manager = InfoDatManager()
        self.info_dat_path = None

    def start(self):
        for source_info in self.source_infos:
            try:
                local_dir, partition_dict, result_file_name = self._make_file_path(source_info)
                local_full_path = os.path.join(local_dir, result_file_name)
                KafkaMessageConsumer(self.interface_info.kafka_proto_info.bootstrap_servers, source_info.kafka_proto_info.topicname, local_full_path,
                                     source_info.kafka_proto_info.filter_pattern, source_info.kafka_proto_info.consuming_timeout).start()
                if os.path.exists(local_full_path):
                    parquet_files = convert(local_full_path, source_info, self.interface_info, None, None)
                    for parquet_file in parquet_files:
                        file_nm = os.path.basename(parquet_file)
                        self._set_info_dat_path()
                        _, ulid = self._send_kafka_msg(partition_dict, source_info, self.collect_history, parquet_file, file_nm)

                        self.info_dat_manager.write_info_dat(parquet_file, partition_dict.get("hdfsPath"), os.path.basename(parquet_file), self.info_dat_path, ulid)
                else:
                    logger.info("No message received...")
            except Exception as e:
                raise Exception(e)

    def _make_file_path(self, source_info):
        match = re.search(r"/db=([^/]+)/tb=([^/]+)", source_info.hdfs_dir)
        if match:
            db = match.group(1)
            tb = match.group(2)

            file_nm = f"{db}.{tb}.csv"
            part_dict = PathUtil.get_partition_info(source_info, self.interface_info.target_time, self.interface_info.interface_cycle, file_nm,
                                                    self.interface_info.start_time)
            local_dir = "/data"+part_dict.get("hdfsPath")
            logger.info("local_dir : %s", local_dir)
            logger.info("hdfs_dir : %s", part_dict.get("hdfsPath"))
            os.makedirs(local_dir, exist_ok=True)
            return local_dir, part_dict, file_nm
        else:
            raise Exception(f"There is no hdfs_dir")

    def _send_kafka_msg(self, partitions, source_info: SourceInfo, collect_history: CollectHistory, local_full_path, file_nm):
        pti = partitions.get("partitions")
        ulid = collect_history.start_collect_h(source_info.collect_source_seq, local_full_path,
                                               partitions.get("hdfsPath"), file_nm, self.interface_info.target_time, pti)
        coll_hist_dict = collect_history.chdDict[ulid]
        self.kafka_send.send_to_kafka(coll_hist_dict)
        return coll_hist_dict, ulid

    def _set_info_dat_path(self):
        new_ulid = ulid.new()
        if not self.info_dat_path:
            self.info_dat_path = Path("/data/gw_meta/test/", self.interface_info.interface_id, "info_" + str(new_ulid) + ".dat")
