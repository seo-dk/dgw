import logging
import os.path

from airflow.exceptions import AirflowException

from commons.ProvideMetaInfoHook import ProvideInterface
from commons.ConvertToCsv import ConvertToCsv
from commons.ProvideFormatConverter import ProvideFormatConverter
from commons.MetaInfoHook import SourceInfo, InterfaceInfo
import commons.HdfsUtil as hu

logger = logging.getLogger(__name__)

class ProvideFileHandler:

    def __init__(self, collect_interface: InterfaceInfo, src_hdfs_path, provide_interface: ProvideInterface, version, clear_path):
        self.collect_interface = collect_interface
        self.provide_interface = provide_interface
        self.src_hdfs_full_path = src_hdfs_path
        self.clear_path = clear_path
        self.version = version

    def start(self, source_info: SourceInfo = None):
        if source_info and self.collect_interface.protocol_cd == 'DISTCP':
            self.set_crypto_provide_path()
            self._adjust_hdfs_path_if_spark_parquet(source_info)
            self._convert_format(source_info)
            self._convert_to_csv()
        else:
            self._convert_format(source_info)
            self._convert_to_csv()
            self._get_raw_file()

    def get_hdfs_path(self):
        logger.info(f"hdfs_path : {self.src_hdfs_full_path}")
        return self.src_hdfs_full_path

    def _convert_format(self, source_info: SourceInfo = None):
        """포맷 변환 메인 메서드"""
        format_converter = ProvideFormatConverter(
            self.src_hdfs_full_path,
            self.provide_interface,
            self.collect_interface
        )

        converted_path = format_converter.execute_convert()
        if converted_path:
            self.src_hdfs_full_path = converted_path

    def _convert_to_csv(self):
        ext_idx = self.src_hdfs_full_path.find(".")
        if ext_idx != -1:
            full_ext = self.src_hdfs_full_path[ext_idx:]
        else:
            full_ext = ""

        if 'csv' in full_ext or 'dat' in full_ext:
            logger.info(f"file extension : {full_ext}, no need to convert to csv")
            return

        ## collected distcp data convert_to_csv
        if self.version == '2' and self.collect_interface.protocol_cd == 'DISTCP':
            logger.info("need to convert to csv")
            self.src_hdfs_full_path = ConvertToCsv(self.src_hdfs_full_path, self.provide_interface, self.clear_path).execute_convert()
        ## collected ftp, dbcon, distcp_batch data convert_to_csv
        elif self.provide_interface.proto_info_json.convert_to == 'csv' and not self.provide_interface.proto_info_json.use_raw_file:
            logger.info(f"{self.provide_interface.interface_id} need to convert to csv")
            logger.info(f"hdfs_path : {self.src_hdfs_full_path}")

            convert_to_csv = ConvertToCsv(self.src_hdfs_full_path, self.provide_interface, self.clear_path)
            converted_path = convert_to_csv.execute_convert()
            logger.info(f"converted_path : {converted_path}")

            if self.collect_interface.protocol_cd == 'DISTCP' and self.collect_interface.interface_cycle != 'OD':
                self.src_hdfs_full_path = converted_path
                return

            local_download_path, local_csv_path, hdfs_path = convert_to_csv.get_path()

            file_names = []

            for file_name in os.listdir(local_csv_path):
                file_path = os.path.join(local_csv_path, file_name)
                if os.path.isfile(file_path):
                    if "_SUCCESS" not in file_name:
                        file_names.append(file_name)

            logger.info(f"file_names : {file_names}")

            if len(file_names) == 1:
                logger.info("%s file founded", file_names[0])
                converted_hdfs_path = os.path.join(hdfs_path, file_names[0])

                ## change hdfs_path to converted hdfs path
                self.src_hdfs_full_path = converted_hdfs_path
            else:
                logger.info("%s of files exist at local_path", len(file_names))
                raise AirflowException(f"need to add merge options or add prevent split option, check converted csv file exist, path: {local_csv_path}")

    def _get_raw_file(self):
        if self.provide_interface.proto_info_json.use_raw_file and not self.provide_interface.proto_info_json.convert_to:
            logger.info("provide raw file")
            if self.provide_interface.protocol_cd == "DISTCP":
                self.src_hdfs_full_path = self.src_hdfs_full_path.replace("/idcube_out", "/staging/raw/idcube_out")
            else:
                raw_staging_path = os.path.dirname(self.src_hdfs_full_path).replace("/idcube_out", "/staging/raw/idcube_out")
                raw_files = hu.get_file_list(raw_staging_path)
                self.src_hdfs_full_path = raw_files[0]
            logger.info(f"get file from {self.src_hdfs_full_path}")

    def _adjust_hdfs_path_if_spark_parquet(self, source_info: SourceInfo):
        if source_info.distcp_proto_info.source_compression and not self.provide_interface.proto_info_json.spark_server_info.is_external:
            self.src_hdfs_full_path = self.src_hdfs_full_path.replace("/staging", '')

    def set_crypto_provide_path(self):
        def remove_file_name_in_path(src_hdfs_full_path):
            basename = os.path.basename(src_hdfs_full_path)
            if '.' in basename and not basename.endswith('.'):
                return os.path.dirname(src_hdfs_full_path)
            else:
                return src_hdfs_full_path

        if not self.provide_interface.proto_info_json.spark_server_info.is_external:
            if "/staging/raw" in self.src_hdfs_full_path and not self.provide_interface.proto_info_json.use_raw_file:
                self.src_hdfs_full_path = self.src_hdfs_full_path.replace("/staging/raw", "", 1)
                self.src_hdfs_full_path = remove_file_name_in_path(self.src_hdfs_full_path)
            elif "/staging/raw" not in self.src_hdfs_full_path and self.provide_interface.proto_info_json.use_raw_file:
                self.src_hdfs_full_path = self.src_hdfs_full_path.replace("/idcube_out", "/staging/raw/idcube_out")
                self.src_hdfs_full_path = remove_file_name_in_path(self.src_hdfs_full_path)
        logger.info(f"source_path : {self.src_hdfs_full_path}")