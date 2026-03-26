import logging
from typing import Union, List
import re
import os

from airflow.models import Variable

from commons.MetaInfoHook import FtpSftpInterfaceProtoInfo, InterfaceInfo, DistcpInterfaceProtoInfo, ColumnCryptoInfo
from commons.ProvideMetaInfoHook import ProvideInterface
from commons.DistcpEncryptManager import DistcpEncryptManager, CryptoConfig
from commons.SparkSubmitLauncher import SparksubmitLauncher, ParquetConversionConfig
from commons.RemoveFileManager import RemoveFileManager


logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))


class RemoteSparkJobManager:
    def __init__(self, interface_info:InterfaceInfo, provide_interface: ProvideInterface, provide_dest_path, protocol):
        self.interface_info = interface_info
        self.provide_interface = provide_interface
        self.provide_dest_path = provide_dest_path
        self.protocol = protocol

    def start(self):
        if self.provide_interface.proto_info_json.crypto_rules.enabled:
            self._encrypt_start()
        elif self.provide_interface.proto_info_json.source_compression == "snappy" and self.provide_interface.proto_info_json.convert_to == "parquet":
            self._convert_start()

    def _encrypt_start(self):
        provide_output_path = self.provide_dest_path
        logger.info(f"encrypt process start at {self.provide_interface.proto_info_json.spark_server_info.ip} server, ")
        logger.info(f"provide_dest_path  {self.provide_dest_path}")
        if "staging" in self.provide_dest_path:
            provide_output_path = self.provide_dest_path.replace("/staging", "")
        logger.info(f"provide_dest_path  {self.provide_dest_path}")

        crypto_rules = self.provide_interface.proto_info_json.crypto_rules
        rules = crypto_rules.rules
        logger.info(f"CryptoRules : {self.provide_interface.proto_info_json.crypto_rules}")
        logger.info(f"provide_interface.proto_info_json.target_format : {self.provide_interface.proto_info_json.target_format}")
        for crypto_type, column_crypto_infos in rules.items():
            if self.protocol == "distcp":
                interface_proto_info = self.interface_info.distcp_proto_info
            else:
                interface_proto_info = self.interface_info.ftp_proto_info
            provide_proto_info = self.provide_interface.proto_info_json

            target_compression = provide_proto_info.target_compression
            if provide_proto_info.no_target_compression:
                target_compression = "none"

            encrypt_config = CryptoConfig(crypto_type, provide_proto_info.spark_server_info.memory_usage, provide_proto_info.source_file_pattern,
                                          self.provide_dest_path, column_crypto_infos, provide_proto_info.format, target_compression,
                                          interface_proto_info.delimiter, interface_proto_info.encode, provide_proto_info.spark_server_info,
                                          provide_proto_info.target_format, provide_proto_info.merge)

            hdfs_partition = f"/db={self.provide_interface.db}/tb={self.provide_interface.tb}"
            logger.info(f"encrypt_config : {encrypt_config}")

            with (DistcpEncryptManager(encrypt_config, hdfs_partition, crypto_rules.is_encrypted, crypto_rules.store_raw_and_crypto, provide_output_path)
                  as distcp_encrypt_manager):
                _, remote_staging_path = distcp_encrypt_manager.start()
                RemoveFileManager(remote_staging_path, "remote").start(provide_proto_info.spark_server_info)

    def _convert_start(self):

        ## provide_dest_path : hdfs://90.90.43.21:8020/tap_d/staging/db=o_datalake/tb=pin_cluster/dt=20250526/
        spark_server_info = self.provide_interface.proto_info_json.spark_server_info

        provide_output_path = self.provide_dest_path.replace("/staging", "")

        tb_match = re.search(r'tb=([^/]+)', self.provide_dest_path)
        tb_name = tb_match.group(1) if tb_match else None
        if self.protocol == "distcp":
            interface_proto_info = self.interface_info.distcp_proto_info

        else:
            interface_proto_info = self.interface_info.ftp_proto_info

        spark_config = ParquetConversionConfig(interface_proto_info.delimiter, self.provide_interface.proto_info_json.format,
                                               "parquet", self.provide_interface.proto_info_json.target_compression,
                                               self.provide_interface.proto_info_json.source_compression, interface_proto_info.encode,
                                               self.provide_interface.proto_info_json.header_skip)

        with SparksubmitLauncher(self.provide_dest_path, provide_output_path, spark_server_info.memory_usage,
                                 tb_name, spark_server_info, "parquet_conversion.py") as spark_launcher:
            _ = spark_launcher.start(spark_config)