from airflow.exceptions import AirflowException
from datetime import datetime
from commons_test.InfodatManager import InfoDatManager
from commons_test.GetDbConnPartition import GetDbConnPartition
import os
import logging
from airflow.models import Variable

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))


class WriteOracleResult:
    def __init__(self, kafka_send, collect_history, meta_info):
        self.kafka_send = kafka_send
        self.collect_history = collect_history
        self.meta_info = meta_info
        self.getDbConnPartition = GetDbConnPartition(self.meta_info)
        self.infodat_IO = InfoDatManager()

    def writing_process(self, local_info_file_path, file_name, result, file_info, current_time, target_dt,
                        target_file_info_dic=None):
        ch_dict = None

        try:
            part_dict = self.getDbConnPartition.get_partition(file_info, target_dt, self.meta_info.interface_cycle)
            partitions = part_dict["partitions"]
            hdfs_path = part_dict["hdfsPath"]

            if partitions is None:
                logger.error("WriteOracleResult partitions is None")
                raise

            collect_source_seq = file_info.collect_source_seq
            local_full_path = "/data" + str(hdfs_path) + "/" + file_name

            ulid = self.create_ulid(target_file_info_dic, partitions, collect_source_seq, local_full_path, hdfs_path,
                                    file_name, current_time)

            if ulid is None:
                logger.error("WriteOracleResult ulid is None")
                raise

            ch_dict = self.collect_history.chdDict[ulid]
            self.kafka_send.send_to_kafka(ch_dict)

            self.write_to_local_file(ch_dict, local_full_path, file_name, result)
            self.write_to_info_file(ch_dict, local_info_file_path, local_full_path, hdfs_path, file_name, ulid)

            return ch_dict

        except Exception as e:
            pass
            logger.error("Error writing_process: %s", e)
            self.kafka_send.sendErrorKafka(ch_dict, 10, False, "Error writing_process")

    def create_ulid(self, target_file_info_dic, partitions, collect_source_seq, local_full_path, hdfs_path, file_name,
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

        return ulid

    def write_to_local_file(self, ch_dict, local_full_path, file_name, result):
        os.makedirs(os.path.dirname(local_full_path), exist_ok=True)

        # write sql result to infoDat
        try:
            with open(local_full_path, 'w') as local_file:
                for row in result:
                    row_str = [str(column).replace('\r', '').replace('\n', '') if column is not None else "null" for
                               column in row]
                    line = "\036".join(row_str)
                    local_file.write(line + "\n")
            logger.info("uploading result to local success, src: %s  path: %s", file_name, local_full_path)

        except Exception as e:
            logger.error("Error uploading result to local success, src: %s  path: %s", file_name, local_full_path)
            logger.error("Error: %s", e)
            self.kafka_send.sendErrorKafka(ch_dict, 10, False, "oracle writing to local failed")

    def write_to_info_file(self, ch_dict, local_info_file_path, local_full_path, hdfs_path, file_name, ulid):
        # write local file path to infoDat
        try:
            os.makedirs(os.path.dirname(local_info_file_path), exist_ok=True)
            self.infodat_IO.write_info_dat(local_full_path, hdfs_path, file_name,
                                           local_info_file_path, ulid)
            logger.info("writing info to InfoDat success, src: %s path: %s", file_name, local_info_file_path)

        except Exception as e:
            logger.error("Error writing info to InfoDat, src: %s path: %s", file_name, local_info_file_path)
            logger.error("Error: %s", e)
            self.kafka_send.sendErrorKafka(ch_dict, 10, False, "oracle writing to info file failed")



