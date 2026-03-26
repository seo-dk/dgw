import logging
from airflow.models import Variable
from typing import List
from datetime import datetime
from airflow.exceptions import AirflowException

from commons_test.DistcpEncryptManager import DistcpEncryptManager, CryptoConfig
from commons_test.DistcpParquetHandler import DistcpParquetHandler
from commons_test.MetaInfoHook import SparkServerInfo, ColumnCryptoInfo, InterfaceInfo, SourceInfo


logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))

class DistcpBatchFileHandler:

    def __init__(self, source_info: SourceInfo, interface_info: InterfaceInfo, hdfs_src_path, coll_hist_dict, kafka_send):
        self.source_info = source_info
        self.interface_info = interface_info
        self.hdfs_src_path = hdfs_src_path
        self.coll_hist_dict = coll_hist_dict
        self.kafka_send = kafka_send

    def start(self):
        if not self.source_info.distcp_proto_info.spark_server_info.is_external:
            self._crypto_start(self.source_info, self.coll_hist_dict)
            self._convert_to_parquet(self.coll_hist_dict)

    def _convert_to_parquet(self, coll_hist_dict):
        if not self.source_info.distcp_proto_info.crypto_rules.enabled and self.source_info.distcp_proto_info.convert_to == "parquet":
            DistcpParquetHandler(self.source_info, self.interface_info, self.hdfs_src_path).start_convert()

            current_time = datetime.now().isoformat()
            coll_hist_dict.dest_path = self.hdfs_src_path.replace("/staging", "")
            coll_hist_dict.ended_at = current_time
            logger.info(f'col_hist_dict: {coll_hist_dict}')

            self.kafka_send.send_to_kafka(coll_hist_dict)
            self.hdfs_src_path = self.hdfs_src_path.replace("/staging", "")

    def _crypto_start(self, source_info: SourceInfo, coll_hist_dict):
        if self.source_info.distcp_proto_info.crypto_rules.enabled and len(self.source_info.distcp_proto_info.crypto_rules.rules) > 0:
            logger.info(f"encrypt start!, spark_server_info : {source_info.distcp_proto_info.spark_server_info}")
            crypto_rules = source_info.distcp_proto_info.crypto_rules
            logger.info(f"crypto_rules : {crypto_rules}")
            rules = crypto_rules.rules

            crypto_hdfs_dest_path = ""

            for crypto_type, column_crypto_infos in rules.items():
                column_crypto_infos: List[ColumnCryptoInfo]
                logger.info(f"{crypto_type} start!!!")

                if source_info.distcp_proto_info.convert_to == "parquet":
                    logger.info("encrypt file will be convert to parquet")
                    crypto_config = self._make_crypto_config(crypto_type, source_info, "hdfs://dataGWCluster" + self.hdfs_src_path,
                                                             SparkServerInfo(), source_info.distcp_proto_info.convert_to, column_crypto_infos)
                else:
                    logger.info("encrypt file format will be same as origin file")
                    crypto_config = self._make_crypto_config(crypto_type, source_info, "hdfs://dataGWCluster" + self.hdfs_src_path,
                                                             SparkServerInfo(), source_info.distcp_proto_info.format, column_crypto_infos)

                with DistcpEncryptManager(crypto_config, source_info.hdfs_dir, crypto_rules.is_encrypted, crypto_rules.store_raw_and_crypto) as distcp_crypto_manager:
                    crypto_hdfs_dest_path, _ = distcp_crypto_manager.start()
                    logger.info(f"crypto_hdfs_dest_path : {crypto_hdfs_dest_path}")

            if not crypto_hdfs_dest_path:
                logger.error("There is no encrypted hdfs path...")
                raise AirflowException("There is no encrypted hdfs path...")

            self.hdfs_src_path = crypto_hdfs_dest_path
            self._send_kafka_message(coll_hist_dict)


    def _send_kafka_message(self, coll_hist_dict):
        current_time = datetime.now().isoformat()
        coll_hist_dict.dest_path = self.hdfs_src_path.replace("/staging", "")
        coll_hist_dict.ended_at = current_time
        logger.info(f'col_hist_dict: {coll_hist_dict}')
        self.kafka_send.send_to_kafka(coll_hist_dict)

    def _make_crypto_config(self, crypto_type, source_info, src_path, spark_server_info, encrypt_format, column_crypto_info: List[ColumnCryptoInfo]) -> CryptoConfig:
        target_compression = source_info.distcp_proto_info.target_compression
        if source_info.distcp_proto_info.no_target_compression:
            target_compression="none"
        return CryptoConfig(crypto_type, source_info.distcp_proto_info.enc_memory_usage, source_info.distcp_proto_info.source_file_pattern,
                            src_path, column_crypto_info, source_info.distcp_proto_info.format, target_compression,
                            self.interface_info.distcp_proto_info.delimiter, self.interface_info.distcp_proto_info.encode, spark_server_info, encrypt_format)