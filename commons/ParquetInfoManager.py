from typing import List,Dict
from dataclasses import dataclass, field
import re
import os.path
import logging

from commons.MetaInfoHook import SourceInfo, InterfaceInfo
from commons.KafkaSend import KafkaSend
from commons.CollectHistory import CollectHistory
from commons.InfodatManager import InfoDatManager

@dataclass
class ParquetMessageInfo:
    collect_source_seq: int = None
    parquet_local_full_path: str = None
    parquet_hdfs_dir: str = None
    parquet_file_nm: str = None
    target_time: str = None
    partition_info: List[Dict[str, str]] = None

logger = logging.getLogger(__name__)
class ParquetInfoManager:

    def __init__(self, source_info: SourceInfo, parquet_local_full_path):
        parts = parquet_local_full_path.split('/')
        parquet_hdfs_dir = '/'.join(parts[2:-1])
        parquet_hdfs_dir = parquet_hdfs_dir.replace("idcube_out/converted", "/idcube_out")
        parquet_file_nm = os.path.basename(parquet_local_full_path)
        self.parquet_msg_info = ParquetMessageInfo(collect_source_seq=source_info.collect_source_seq, parquet_local_full_path=parquet_local_full_path,
                                                   parquet_hdfs_dir=parquet_hdfs_dir, parquet_file_nm=parquet_file_nm)

    def complete_msg_info(self, target_time, partition):
        self.parquet_msg_info.target_time = target_time
        self.parquet_msg_info.partition_info = partition

    def send_kafka(self, collect_history: CollectHistory):
        kafka_send = KafkaSend()
        ulid = collect_history.start_collect_h(self.parquet_msg_info.collect_source_seq, self.parquet_msg_info.parquet_local_full_path,
                                               self.parquet_msg_info.parquet_hdfs_dir, self.parquet_msg_info.parquet_file_nm,
                                               self.parquet_msg_info.target_time, self.parquet_msg_info.partition_info)

        coll_hist_dict = collect_history.chdDict[ulid]
        kafka_send.send_to_kafka(coll_hist_dict)
        return ulid

    def write_to_info_dat(self, info_dat_path, ulid):
        info_dat_manager = InfoDatManager()

        logger.info("parquet_local_path : %s", self.parquet_msg_info.parquet_local_full_path)
        logger.info("parquet_info.parquet_hdfs_dir : %s", self.parquet_msg_info.parquet_hdfs_dir)
        logger.info("parquet_info.parquet_file_name : %s", self.parquet_msg_info.parquet_file_nm)

        info_dat_manager.write_info_dat(self.parquet_msg_info.parquet_local_full_path, self.parquet_msg_info.parquet_hdfs_dir,
                                        self.parquet_msg_info.parquet_file_nm, info_dat_path, ulid)
        info_dat_manager.delete_non_parquet(info_dat_path)