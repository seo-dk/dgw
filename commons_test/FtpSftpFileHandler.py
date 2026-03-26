import logging
import os
from datetime import datetime
from pathlib import Path

from commons_test.DelimiterChanger import DelimiterChanger
from commons_test.ExcelToCsv import ExcelToCsv
from commons_test.InfodatManager import InfoDatManager
from commons_test.MetaInfoHook import InterfaceInfo, SourceInfo
from commons_test.CollectHistory import CollectHistory
from commons_test.PathUtil import PathUtil, LocalPathInfos
from commons_test.ConvertToParquet2_v2 import convert
from commons_test.CompressionManager import CompressionManager
from commons_test.MergeToParquet import MergeToParquet
from commons_test.KafkaSend import KafkaSend
from commons_test.CryptoExecutor import CryptoExecutor
from commons_test.ColumnRemover import ColumnRemover

logger = logging.getLogger(__name__)

class FtpSftpFileHandler:

    def __init__(self, kafka_send: KafkaSend):

        self.kafka_send = kafka_send
        self.info_dat_manager = InfoDatManager()
        self.delimiter_changer = DelimiterChanger()
        self.compression_manager = CompressionManager()
        self.is_header_skip = False

    def run(self, interface_info: InterfaceInfo, collect_history: CollectHistory, collected_file_info):

        source_info = collected_file_info.source_info
        local_path_infos = collected_file_info.local_path_infos
        filename = collected_file_info.match_file_nm
        coll_hist_dict = collected_file_info.coll_hist_dict
        partitions = collected_file_info.partitions
        ulid = collected_file_info.ulid

        self.is_header_skip = source_info.file_proto_info.header_skip or interface_info.ftp_proto_info.header_skip

        self._convert_excel_to_csv(local_path_infos, filename, coll_hist_dict, collect_history, partitions, ulid, interface_info)

        self._extract_from_tar(source_info, local_path_infos.local_tmp_full_path, coll_hist_dict, collect_history, filename, partitions, local_path_infos, ulid, interface_info)

        self._execute_file_encryption_and_decryption(source_info, interface_info, collected_file_info)

        self._delete_columns(source_info, local_path_infos, interface_info)

        self.delimiter_changer.replace(local_path_infos.local_tmp_full_path, interface_info)

        self._save_without_header(interface_info, local_path_infos.local_tmp_full_path)

        self._write_to_infodat(interface_info, source_info, local_path_infos, partitions, ulid)

        self._convert_to_parquet(local_path_infos, source_info, interface_info, collect_history, partitions, ulid)

    def check_external_process(self, interface_info: InterfaceInfo, source_info: SourceInfo, local_path_infos: LocalPathInfos, filename):
        if interface_info.ftp_proto_info.external_process:
            local_path_infos.local_tmp_full_path = Path(interface_info.ftp_proto_info.ramdisk_dir, "in", filename)
        else:
            local_path_infos.local_tmp_full_path = self._add_dirpath_to_filenm(local_path_infos, filename, interface_info, source_info)

    def _write_to_infodat(self, interface_info: InterfaceInfo, source_info: SourceInfo, local_path_infos: LocalPathInfos, partitions, ulid):
        filename = os.path.basename(local_path_infos.local_tmp_full_path)

        if (
                source_info.file_proto_info.merge
                and not filename.endswith(".tar")
                and source_info.file_proto_info.convert_to
        ):
            local_path_infos.local_tmp_full_path = str(local_path_infos.local_tmp_full_path) + ".merge"
            self.info_dat_manager.write_info_dat(local_path_infos.local_tmp_full_path, partitions.get("hdfsPath"), filename, local_path_infos.local_info_data_path, ulid)
            logger.info("change local file name to %s", local_path_infos.local_tmp_full_path)

        if (
                not interface_info.ftp_proto_info.external_process
                and not filename.endswith(".tar")
                and interface_info.ftp_proto_info.format != "xlsx"
                and not source_info.file_proto_info.convert_to
        ):
            self.info_dat_manager.write_info_dat(local_path_infos.local_tmp_full_path, partitions.get("hdfsPath"), filename, local_path_infos.local_info_data_path, ulid)

    def _convert_to_parquet(self, local_path_infos: LocalPathInfos, source_info: SourceInfo, interface_info: InterfaceInfo, collect_history: CollectHistory, partitions, ulid):
        try:
            if source_info.file_proto_info.convert_to == "parquet" and not source_info.file_proto_info.merge:
                logger.info("convert to parquet start")
                if not source_info.file_proto_info.merge:
                    parquet_files = convert(str(local_path_infos.local_tmp_full_path), source_info, interface_info, collect_history, local_path_infos.local_info_data_path)
                    for parquet_file in parquet_files:
                        logger.info("parquet_files: %s", parquet_file)
                        collect_history.chdDict[ulid].local_path = parquet_file
                        collect_history.chdDict[ulid].filenm = os.path.basename(parquet_file)
                        self.kafka_send.send_to_kafka(collect_history.chdDict[ulid])
                        self.info_dat_manager.write_info_dat(parquet_file, partitions.get("hdfsPath"), os.path.basename(parquet_file), local_path_infos.local_info_data_path, ulid)
        except Exception as e:
            raise Exception(e)

    def _extract_from_zip(self, interface_info: InterfaceInfo, source_info: SourceInfo, local_tmp_full_path, partitions,
                          local_path_infos: LocalPathInfos, collect_history: CollectHistory, ulid):
        if str(local_tmp_full_path).endswith(".zip"):

            end_time = datetime.now()
            current_time = end_time.isoformat()
            collect_history.chdDict[ulid].dest_path = ""
            chd = collect_history.chdDict[ulid]
            collect_history.set_partition_info(ulid, os.path.basename(local_tmp_full_path), os.path.getsize(local_tmp_full_path), current_time, local_tmp_full_path)
            collect_history.set_status_code(chd, 4)
            self.kafka_send.send_to_kafka(chd)

            extract_file_list = self.compression_manager.decompress_zip_file(local_tmp_full_path)
            for extract_file in extract_file_list:
                extract_file_name = os.path.basename(extract_file)
                logger.info("sending .zip extract file kafka message")

                pti = partitions.get("partitions")
                ulid = collect_history.start_collect_h(source_info.collect_source_seq, extract_file,
                                                       partitions.get("hdfsPath"), extract_file_name,
                                                       source_info.file_proto_info.target_time, pti)
                coll_hist_dict = collect_history.chdDict[ulid]
                self.kafka_send.send_to_kafka(coll_hist_dict)

                self._save_without_header(interface_info, extract_file)
            # convert(local_tmp_path_pattern, source_info, interface_info, collect_history, full_path_dict.get("infoDatPath"))
                self.info_dat_manager.write_info_dat(extract_file, partitions.get("hdfsPath"), extract_file_name, local_path_infos.local_info_data_path, ulid)

    def _convert_excel_to_csv(self, local_path_infos: LocalPathInfos, filename, chd, collect_history: CollectHistory, partitions, ulid, interface_info: InterfaceInfo):
        if interface_info.ftp_proto_info.format == "xlsx":
            csv_paths = []
            try:
                logger.info("xlsx file convert start")
                logger.info("local_path : %s", local_path_infos.local_tmp_full_path)
                excel_to_csv = ExcelToCsv(local_path_infos.local_tmp_full_path)
                csv_paths = excel_to_csv.convert()
                if csv_paths:
                    logger.info("xlsx to csv convert finished")
                    for csv_path in csv_paths:
                        self.delimiter_changer.replace(csv_path, interface_info)
                        filename = os.path.basename(csv_path)
                        self.info_dat_manager.write_info_dat(csv_path, partitions.get("hdfsPath"), filename,
                                                             local_path_infos.local_info_data_path, ulid)
                else:
                    logger.warning("csv file was not made")
            except Exception as e:
                chd.err_msg = "filenm : {}, {}".format(filename, str(e)[:1024])
                if csv_paths:
                    collect_history.set_status_code(chd, 2)
                else:
                    collect_history.set_status_code(chd, 5)
                chd.ended_at = datetime.now().isoformat()
                # send error message to kafka
                self.kafka_send.send_to_kafka(chd)
                raise Exception("Error FTP get, file : %s" % filename)

    def _extract_from_tar(self, source_info: SourceInfo, tar_full_path, chd, collect_history: CollectHistory, filename, partitions, local_path_infos: LocalPathInfos, ulid,
                          interface_info: InterfaceInfo):
        if filename.endswith(".tar"):
            local_tar_dir = Path(os.path.dirname(tar_full_path), "tar_tmp")
            os.makedirs(local_tar_dir, exist_ok=True)
            try:
                extracted_files = self.compression_manager.decompress_tar_file(tar_full_path, local_tar_dir)
                for extract_file in extracted_files:
                    if source_info.file_proto_info.merge and source_info.file_proto_info.convert_to:
                        merge_extract_file = str(extract_file) + ".merge"
                        self.info_dat_manager.write_info_dat(merge_extract_file, partitions.get("hdfsPath"), os.path.basename(merge_extract_file),
                                                             local_path_infos.local_info_data_path, ulid)
                    else:
                        self.info_dat_manager.write_info_dat(extract_file, partitions.get("hdfsPath"), os.path.basename(extract_file),
                                                             local_path_infos.local_info_data_path, ulid)
                if source_info.file_proto_info.merge and source_info.file_proto_info.convert_to:
                    merge_to_parquet = MergeToParquet(local_path_infos.local_info_data_path, interface_info, source_info, collect_history, self.kafka_send)
                    merge_parquet_file_name = merge_to_parquet.parquet_info.parquet_file_name
                    partitions = PathUtil.get_partition_info(source_info, source_info.file_proto_info.target_time, interface_info.interface_cycle, merge_parquet_file_name,
                                                             interface_info.start_time)
                    merge_to_parquet.set_partition_info(partitions)
                    merge_to_parquet.start_merge()
            except Exception as e:
                chd.err_msg = "filename : {}, {}".format(filename, str(e)[:1024])
                collect_history.set_status_code(chd, 2)
                chd.ended_at = datetime.now().isoformat()
                # send error message to kafka
                self.kafka_send.send_to_kafka(chd)
                raise Exception("Error FTP get, tar_path: %s, file :%s" % (tar_full_path, filename))

    def _add_dirpath_to_filenm(self, local_path_infos: LocalPathInfos, file_nm, interface_info: InterfaceInfo, source_info: SourceInfo):
        if interface_info.ftp_proto_info.dir_extract_pattern:
            new_file_nm = PathUtil.extract_dir_and_combine_to_filenm(interface_info, file_nm)
            dir_path = os.path.dirname(local_path_infos.local_tmp_full_path)
            new_path = Path(dir_path, new_file_nm)
            logger.info("New local path: " + str(new_path))
            return new_path
        elif source_info.file_proto_info.file_prefix or interface_info.ftp_proto_info.file_prefix:
            prefix = interface_info.ftp_proto_info.file_prefix if interface_info.ftp_proto_info.file_prefix else source_info.file_proto_info.file_prefix
            dir_path = os.path.dirname(local_path_infos.local_tmp_full_path)
            new_path = Path(dir_path, f"{prefix}{file_nm}")
            logger.info("New local path: " + str(new_path))
            return new_path
        else:
            return local_path_infos.local_tmp_full_path

    def _save_without_header(self, interface_info: InterfaceInfo, local_full_path):
        if not self.is_header_skip:
            logger.info("no need to skip")
            return
        if os.path.getsize(local_full_path) == 0:
            logger.info("empty file...nothing to skip")
            return
        logger.info("local_full_path : %s", local_full_path)
        if interface_info.ftp_proto_info.format == 'gz':
            compression_manager = CompressionManager()
            local_full_path = compression_manager.decompress_gz_file(local_full_path)
        tmp_file_path = str(local_full_path) + ".tmp"
        with open(local_full_path, 'r', encoding=interface_info.ftp_proto_info.encode) as local_file:
            next(local_file)
            with open(tmp_file_path, 'w', encoding=interface_info.ftp_proto_info.encode) as tmp_file:
                for line in local_file:
                    tmp_file.write(line)
        if os.path.exists(tmp_file_path):
            os.rename(tmp_file_path, local_full_path)
        logger.info("header removed successfully..")

    def _execute_file_encryption_and_decryption(self, source_info: SourceInfo, interface_info: InterfaceInfo, collected_file_info):
        try:
            crypto_executor = CryptoExecutor(self.is_header_skip, interface_info.ftp_proto_info.encode, interface_info.ftp_proto_info.delimiter,
                                             source_info.file_proto_info.crypto_rules)

            crypto_executor.exec(collected_file_info.local_path_infos.local_tmp_full_path)
            if source_info.file_proto_info.crypto_rules.enabled and self.is_header_skip:
                self.is_header_skip = False
        except Exception as e:
            raise

    def _delete_columns(self, source_info: SourceInfo, local_path_infos: LocalPathInfos, interface_info: InterfaceInfo):
        if source_info and source_info.file_proto_info.delete_columns:
            ColumnRemover(local_path_infos.local_tmp_full_path, source_info.file_proto_info.delete_columns, interface_info.ftp_proto_info.delimiter).start()

    def _write_origin_file_to_info_dat(self, interface_info, local_tmp_full_path, hdfs_path, info_dat_path, ulid):
        if (not interface_info.ftp_proto_info.external_process and not os.path.basename(local_tmp_full_path).endswith(".tar")
                and interface_info.ftp_proto_info.format != "xlsx"):
            self.info_dat_manager.write_info_dat(local_tmp_full_path, hdfs_path, os.path.basename(local_tmp_full_path),
                                                 info_dat_path, ulid)

    def _set_origin_file_to_send_provide(self, interface_info, local_path_infos: LocalPathInfos, collect_history, partitions, ulid):
        coll_hist_dict = collect_history.chdDict[ulid]
        local_tmp_full_path = local_path_infos.local_tmp_full_path
        hdfs_path = partitions.get("hdfsPath").replace("idcube_out", "provide")

        new_ulid = collect_history.start_collect_h(coll_hist_dict.collect_source_seq, local_tmp_full_path,
                                                   hdfs_path, coll_hist_dict.filenm, coll_hist_dict.target_dt, coll_hist_dict.partitions)
        coll_hist_dict = collect_history.chdDict[new_ulid]
        self.kafka_send.send_to_kafka(coll_hist_dict)

        info_dat_path = local_path_infos.local_info_data_path
        self._write_origin_file_to_info_dat(interface_info, local_tmp_full_path, hdfs_path, info_dat_path, new_ulid)