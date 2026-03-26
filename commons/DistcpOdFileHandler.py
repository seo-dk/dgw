import logging
from datetime import datetime
from typing import List

from airflow.exceptions import AirflowException
from airflow.models import Variable

from commons.DistcpParquetHandler import DistcpParquetHandler
from commons.MetaInfoHook import SparkServerInfo, ColumnCryptoInfo, InterfaceInfo, SourceInfo
from commons.DistcpEncryptManager import DistcpEncryptManager, CryptoConfig
from commons.KafkaSend import KafkaSend
from commons.DistcpExecutor import DistcpExecutor

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))

class DistcpOdFileHandler:
    def __init__(self, interface_info: InterfaceInfo, source_info: SourceInfo, kafka_send: KafkaSend, coll_hist_dict, hdfs_src_path):
        self.interface_info = interface_info
        self.source_info = source_info
        self.kafka_send = kafka_send
        self.coll_hist_dict = coll_hist_dict
        self.hdfs_src_path = hdfs_src_path

    def start(self):
        if not self.source_info.distcp_proto_info.spark_server_info.is_external:
            self._encrypt_start(self.source_info, self.coll_hist_dict)
            self._convert_to_parquet(self.source_info, self.coll_hist_dict)
            self._rename_file_name()

    def _rename_file_name(self):
        if self.source_info.distcp_proto_info.file_rename:
            with DistcpExecutor() as distcp_executor:
                logger.info("source_info.distcp_proto_info.file_nm: %s", self.source_info.distcp_proto_info.file_nm)
                compression = "zstd"
                format = "parquet"
                if self.source_info.distcp_proto_info.target_compression:
                    compression = self.source_info.distcp_proto_info.target_compression
                if self.source_info.distcp_proto_info.format:
                    format = self.source_info.distcp_proto_info.format
                distcp_executor.exec_rename_file(self.hdfs_src_path, self.source_info.distcp_proto_info.file_nm, f"{compression}.{format}")

    def _convert_to_parquet(self, source_info, coll_hist_dict):
        if not self.source_info.distcp_proto_info.crypto_rules.enabled and self.source_info.distcp_proto_info.convert_to == "parquet":
            DistcpParquetHandler(source_info, self.interface_info, self.hdfs_src_path).start_convert()

            current_time = datetime.now().isoformat()
            coll_hist_dict.dest_path = self.hdfs_src_path.replace("/staging", "")
            coll_hist_dict.ended_at = current_time
            logger.info(f'col_hist_dict: {coll_hist_dict}')

            self.kafka_send.send_to_kafka(coll_hist_dict)
            self.hdfs_src_path = self.hdfs_src_path.replace("/staging", "")

    def _encrypt_start(self, source_info: SourceInfo, coll_hist_dict):
        if self.source_info.distcp_proto_info.crypto_rules.enabled and len(self.source_info.distcp_proto_info.crypto_rules.rules) > 0:
            logger.info(f"encrypt start!, spark_server_info : {source_info.distcp_proto_info.spark_server_info}")
            crypto_rules = source_info.distcp_proto_info.crypto_rules
            logger.info(f"crypto_rules : {crypto_rules}")
            rules = crypto_rules.rules

            crypto_hdfs_dest_path = ""

            for crypto_type, column_crypto_infos in rules.items():
                column_crypto_infos: List[ColumnCryptoInfo]
                logger.info(f"{crypto_type} start!!!")

                if source_info.distcp_proto_info.convert_to == "parquet" or source_info.distcp_proto_info.convert_to == "csv":
                    logger.info(f"encrypt file will be convert to {source_info.distcp_proto_info.convert_to}")
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
            target_compression = "none"
        return CryptoConfig(crypto_type, source_info.distcp_proto_info.enc_memory_usage, source_info.distcp_proto_info.source_file_pattern,
                            src_path, column_crypto_info, source_info.distcp_proto_info.format, target_compression,
                            self.interface_info.distcp_proto_info.delimiter, self.interface_info.distcp_proto_info.encode, spark_server_info, encrypt_format,
                            source_info.distcp_proto_info.merge)
