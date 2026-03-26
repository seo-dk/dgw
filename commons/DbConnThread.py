import logging
import os
from datetime import datetime

from airflow.models import Variable

from commons.DbConnWriter import DbConnWriter
from commons.GetDbConnPartition import GetDbConnPartition
from commons.MetaInfoHook import InterfaceInfo, SourceInfo
from commons.ConvertToParquet2_v2 import convert
from commons.CollectHistory import CollectHistory
from commons.InfodatManager import InfoDatManager

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))


class DbConnThread:
    def __init__(self, kafka_send, collect_history: CollectHistory, db_hook, interface_info: InterfaceInfo, local_info_file_path):
        self.kafka_send = kafka_send
        self.collect_history = collect_history
        self.db_hook = db_hook
        self.interface_info = interface_info
        self.local_info_file_path = local_info_file_path
        self.db_conn_writer = DbConnWriter()
        self.getDbConnPartition = GetDbConnPartition(self.interface_info)

    def run(self, query, sql_params, file_name, file_info: SourceInfo, current_time, target_time, index,
            target_file_info_dic=None):
        ulid = None

        try:
            # TMS_SUBWAY_AREA_QUALITY_202307261420_0.dat
            file_name = file_name + str(index) + '.dat'
            logger.info("DBCONNThread start, file_info.COLLECT_SOURCE_SEQ: %s, fileName: %s", file_info.collect_source_seq, file_name)

            partition_dict = self._create_partition_dict(file_info, target_time, self.interface_info, file_name)
            partitions = partition_dict["partitions"]
            hdfs_path = partition_dict["hdfsPath"]

            local_full_path = "/data" + str(hdfs_path) + "/" + file_name

            collect_source_seq = file_info.collect_source_seq

            ulid = self._create_ulid(target_file_info_dic, partitions, collect_source_seq, local_full_path, hdfs_path,
                                     file_name, current_time)
            collect_history_dict = self._create_collect_history_dict(file_name, ulid)

            start = datetime.now()

            self.db_hook.executeDataSql(collect_history_dict, query, sql_params, local_full_path, file_name)
            diff = datetime.now() - start
            logger.info("executing Sql and writing to local done, collect_history_dict: %s, file_name: %s, elapsed time: %s", ulid, file_name, diff)

            self.db_conn_writer.write_to_info_file(self.kafka_send, collect_history_dict, self.local_info_file_path, local_full_path, hdfs_path, file_name, ulid)

            if file_info.db_proto_info.convert_to == "parquet":
                self._converting_to_parquet(collect_history_dict, file_info, partitions, local_full_path, hdfs_path, ulid)

            return collect_history_dict

        except Exception as e:
            logger.exception("DBCONNThread run collect_hist_id: %s, file %s, error: %s", ulid, file_name, e)

    def _create_partition_dict(self, file_info, target_dt, meta_info, file_name):
        partition_dict = self.getDbConnPartition.get_partition(file_info, target_dt, meta_info.interface_cycle)

        # YYYYMM01
        if file_info.db_proto_info.partitions:
            partition_dict = self.getDbConnPartition.get_overwritten_path(partition_dict)

        if partition_dict["partitions"] is None:
            logger.error("partition_dict's partitions value is None, file_name: %s", file_name)
            raise

        elif partition_dict["hdfsPath"] is None:
            logger.error("partition_dict's hdfs_path value is None, file_name: %s", file_name)
            raise

        return partition_dict

    def _create_ulid(self, target_file_info_dic, partitions, collect_source_seq, local_full_path, hdfs_path, file_name,
                     current_time):
        # retry
        if target_file_info_dic:
            ulid = self.collect_history.start_collect_h(collect_source_seq, local_full_path, hdfs_path,
                                                        file_name, current_time, partitions,
                                                        target_file_info_dic['collect_hist_id'],
                                                        target_file_info_dic['started_at'],
                                                        target_file_info_dic['retry_id_seq'])
        else:
            ulid = self.collect_history.start_collect_h(collect_source_seq, local_full_path, hdfs_path,
                                                        file_name, current_time, partitions)

        if ulid is None:
            logger.error("ulid is None file_name: %s", file_name)
            raise

        return ulid

    def _create_collect_history_dict(self, file_name, ulid):
        collect_history_dict = self.collect_history.chdDict[ulid]
        self.kafka_send.send_to_kafka(collect_history_dict)

        if collect_history_dict is None:
            logger.error("collect_history_dict file_name: %s, collect_hist_id: %s", file_name, ulid)
            raise

        return collect_history_dict

    def _converting_to_parquet(self, collect_history_dict, file_info: SourceInfo, partitions, local_file_path, hdfs_path, ulid):
        logger.info("need to convert to parquet")

        parquet_files = convert(local_file_path, file_info, self.interface_info, None, None)
        logger.info("parquet_files: %s", parquet_files)
        infodat_io = InfoDatManager()
        infodat_io.reset_infodat(self.local_info_file_path)
        for parquet_file in parquet_files:
            collect_history_dict.local_path = parquet_file
            collect_history_dict.filenm = os.path.basename(parquet_file)
            self._create_collect_history_dict(parquet_file, ulid)
            self.db_conn_writer.write_to_info_file(self.kafka_send, collect_history_dict, self.local_info_file_path,
                                                   parquet_file, hdfs_path, collect_history_dict.filenm, ulid)



