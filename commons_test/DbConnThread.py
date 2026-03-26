import logging
import os
from datetime import datetime

from airflow.models import Variable

from commons_test.DbConnWriter import DbConnWriter
from commons_test.MetaInfoHook import InterfaceInfo, SourceInfo
from commons_test.ConvertToParquet2_v2 import convert
from commons_test.CollectHistory import CollectHistory

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))


class DbConnThread:
    def __init__(self, get_db_conn_collect_hist, kafka_send, collect_history: CollectHistory, db_hook, interface_info: InterfaceInfo, local_info_file_path):
        self.get_db_conn_collect_hist = get_db_conn_collect_hist
        self.kafka_send = kafka_send
        self.collect_history = collect_history
        self.db_hook = db_hook
        self.interface_info = interface_info
        self.local_info_file_path = local_info_file_path
        self.db_conn_writer = DbConnWriter()

    def run(self, query, sql_params, file_name, file_info: SourceInfo, current_time, target_time, index,
            target_file_info_dic=None):
        ulid = None

        try:
            # TMS_SUBWAY_AREA_QUALITY_202307261420_0.dat
            file_name = file_name + str(index) + '.dat'
            logger.info("DBCONNThread start, file_info.COLLECT_SOURCE_SEQ: %s, fileName: %s", file_info.collect_source_seq, file_name)

            # partition_dict = self._create_partition_dict(file_info, target_time, self.interface_info, file_name)
            partition_dict = self.get_db_conn_collect_hist.create_partition_dict(file_info, target_time, file_name)
            partitions = partition_dict["partitions"]
            hdfs_path = partition_dict["hdfsPath"]

            local_full_path = "/data" + str(hdfs_path) + "/" + file_name

            collect_source_seq = file_info.collect_source_seq

            ulid = self.get_db_conn_collect_hist.create_ulid(target_file_info_dic, partitions, collect_source_seq, local_full_path, hdfs_path,
                                                                file_name, current_time)

            collect_history_dict = self.get_db_conn_collect_hist.create_collect_history_dict(file_name, ulid)
            self.kafka_send.send_to_kafka(collect_history_dict)

            start = datetime.now()

            self.db_hook.executeDataSql(collect_history_dict, query, sql_params, local_full_path, file_name)
            diff = datetime.now() - start
            logger.info("executing Sql and writing to local done, collect_history_dict: %s, file_name: %s, elapsed time: %s", ulid, file_name, diff)

            if file_info.db_proto_info.convert_to == "parquet":
                if not file_info.db_proto_info.merge:
                    self._converting_to_parquet(collect_history_dict, file_info, local_full_path, hdfs_path, ulid)
                    return collect_history_dict
                else:
                    return

            self.db_conn_writer.write_to_info_file(self.kafka_send, collect_history_dict, self.local_info_file_path, local_full_path, hdfs_path, file_name, ulid)

            return collect_history_dict

        except Exception as e:
            logger.exception("DBCONNThread run collect_hist_id: %s, file %s, error: %s", ulid, file_name, e)

    def _converting_to_parquet(self, collect_history_dict, file_info: SourceInfo, local_file_path, hdfs_path, ulid):
        logger.info("need to convert to parquet")

        parquet_file = convert(local_file_path, file_info, self.interface_info, None, None)
        logger.info("parquet_file: %s", parquet_file)

        collect_history_dict.local_path = parquet_file
        collect_history_dict.filenm = os.path.basename(parquet_file)
        self.get_db_conn_collect_hist.create_collect_history_dict(parquet_file, ulid)

        self.db_conn_writer.write_to_info_file(self.kafka_send, collect_history_dict, self.local_info_file_path,
                                               parquet_file, hdfs_path, collect_history_dict.filenm, ulid)
