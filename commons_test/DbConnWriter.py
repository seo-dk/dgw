import logging
import os
from airflow.models import Variable
from commons_test.InfodatManager import InfoDatManager

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))


class DbConnWriter:
    def remove_local_file(self, local_full_path):
        if os.path.isfile(local_full_path):
            os.remove(local_full_path)
            logger.debug("local file already exist, it has removed")

    def write_to_local_file(self, kafka_send, collect_history_dict, local_full_path, file_name, sql_result, total_batch):
        os.makedirs(os.path.dirname(local_full_path), exist_ok=True)
        # write sql sql_result to infoDat
        try:
            with open(local_full_path, 'a') as local_file:
                if not sql_result:
                    logger.info("%s file, query result count is 0", file_name)
                    return

                for row in sql_result:
                    row_str = [str(column).replace('\r', '').replace('\n', '') if column is not None else "null" for
                               column in row]
                    line = "\036".join(row_str)
                    local_file.write(line + "\n")

            if total_batch % 250000 == 0:
                logger.debug("writing sql_result to local, collect_history_dict.id: %s, total_batch: %s, src: %s, path: %s", collect_history_dict.collect_hist_id, total_batch,
                             file_name, local_full_path)
        except Exception as e:
            logger.error("Error writing sql_result to local, collect_history_dict.id: %s, src: %s,  path: %s, error: %s", collect_history_dict.collect_hist_id, file_name,
                         local_full_path, e)
            kafka_send.sendErrorKafka(collect_history_dict, 10, False, "oracle writing to local failed")

    def write_to_info_file(self, kafka_send, collect_history_dict, local_info_file_path, local_full_path, hdfs_path, file_name, ulid):
        infodat_io = InfoDatManager()
        # write local file path to infoDat
        try:
            os.makedirs(os.path.dirname(local_info_file_path), exist_ok=True)
            infodat_io.write_info_dat(local_full_path, hdfs_path, file_name, local_info_file_path, ulid)
            logger.info("writing info to InfoDat finished, collect_history_dict.id: %s, src: %s path: %s", collect_history_dict.collect_hist_id, file_name, local_info_file_path)

        except Exception as e:
            logger.error("Error writing info to InfoDat, collect_history_dict.id: %s, src: %s, path: %s, error: %s", collect_history_dict.collect_hist_id, file_name,
                         local_info_file_path, e)
            kafka_send.sendErrorKafka(collect_history_dict, 10, False, "oracle writing to info file failed")
