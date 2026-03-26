import concurrent.futures
import logging
import multiprocessing
from typing import List

from commons.MetaInfoHook import InterfaceInfo, SourceInfo
from commons.Util import Util
from commons.PathUtil import PathUtil
from commons.DatabaseHook import DatabaseHook
from commons.DbConnThread import DbConnThread
from commons.CollectHistory import CollectHistory

logger = logging.getLogger(__name__)


class HandleDbConnList:

    def __init__(self, formatted_execution_date, kafka_send, collect_history: CollectHistory, interface_info: InterfaceInfo, file_list_info: List[SourceInfo], local_info_file_path, collect_file_info_list=None):
        self.formatted_execution_date = formatted_execution_date
        self.kafka_send = kafka_send
        self.collect_history = collect_history
        self.interface_info = interface_info
        self.file_list_info = file_list_info
        self.local_info_file_path = local_info_file_path
        self.collect_file_info_list = collect_file_info_list
        self.retry = False
        self.db_hook = DatabaseHook(self.kafka_send, self.collect_history, interface_info.db_proto_info)

    def handle_file_list(self):
        parse = Util()
        dbconn_thread = DbConnThread(self.kafka_send, self.collect_history, self.db_hook,
                                     self.interface_info, self.local_info_file_path)

        if self.collect_file_info_list is not None:
            self.retry = True

        try:
            for file_info in self.file_list_info:
                retry_target_file_info_dic = None

                if self.retry:
                    retry_target_file_info_dic = self._get_retry_target_file_info_dic(file_info.collect_source_seq)
                    current_time = retry_target_file_info_dic["target_dt"]
                else:
                    current_time = self.formatted_execution_date

                logger.info("current_time: %s", current_time)

                # get count sql result
                param_reuslt = Util.get_sql_params(file_info.db_proto_info.count_params, current_time)
                record_count = self.db_hook.executeCountSql(file_info.db_proto_info.count_query,
                                                                param_reuslt)
                logger.info("file_info.COLLECT_SOURCE_SEQ: %s, param_reuslt: %s, record_count: %s", file_info.collect_source_seq, param_reuslt, record_count)

                # get data sql's params
                param_reuslt = Util.get_sql_params(file_info.db_proto_info.data_params, current_time)
                num_rows = int(file_info.db_proto_info.num_rows)
                logger.info("file_info.COLLECT_SOURCE_SEQ: %s, param_reuslt: %s, num_rows: %s", file_info.collect_source_seq, param_reuslt, num_rows)

                row_num_list = Util.get_row_num_list(record_count, num_rows)
                query_list = Util.get_data_query_list(row_num_list, num_rows, file_info.db_proto_info.data_query, record_count, self.interface_info.db_proto_info)
                logger.info("file_info.COLLECT_SOURCE_SEQ: %s, row_num_list: %s, query_list's length : %s", file_info.collect_source_seq, row_num_list, len(query_list))

                # get latest date
                target_time = Util.get_oracle_target_time(param_reuslt)
                logger.info("file_info.COLLECT_SOURCE_SEQ: %s, target_time: %s", file_info.collect_source_seq, target_time)

                # TMS_SUBWAY_AREA_QUALITY_202307261420_
                file_name = PathUtil.get_oracle_file_name(file_info, target_time)
                logger.info("file_info.COLLECT_SOURCE_SEQ: %s, file_name: %s", file_info.collect_source_seq, file_name)

                # multi thread
                max_worker = multiprocessing.cpu_count() // 4
                total_worker = len(query_list)
                worker = min(total_worker, max_worker)

                future_list = []
                logger.info('Thread start')

                with concurrent.futures.ThreadPoolExecutor(max_workers=worker) as executor:
                    # execute query and write to local
                    for index, query in enumerate(query_list):
                        if self.retry:
                            future = executor.submit(dbconn_thread.run, query, param_reuslt, file_name,
                                                     file_info, current_time, target_time, index,
                                                     retry_target_file_info_dic)
                        else:
                            future = executor.submit(dbconn_thread.run, query, param_reuslt, file_name, file_info, current_time, target_time, index, None)

                        future_list.append(future)
                logger.info('Thread finished')

            self.db_hook.closeConnection()

        except Exception as e:
            logger.error("failed handle_file_list: %s", e)
            self.kafka_send.sendErrorKafka(self.collect_history, 10, True, "failed handle_file_list")

    def _get_retry_target_file_info_dic(self, collect_source_seq):
        target_file_info_dic = None

        for key, value in self.collect_file_info_list.items():
            if value.get('collect_source_seq') == str(collect_source_seq):
                target_collect_hist_id = key
                target_file_info_dic = self.collect_file_info_list[str(target_collect_hist_id)]

                del self.collect_file_info_list[str(target_collect_hist_id)]
                break

        return target_file_info_dic
