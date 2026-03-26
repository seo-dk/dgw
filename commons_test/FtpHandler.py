import logging
import os
from datetime import datetime
from pathlib import Path
from ftplib import FTP
from paramiko import SFTPClient

from commons_test.DelimiterChanger import DelimiterChanger
from commons_test.ExcelToCsv import ExcelToCsv
from commons_test.InfodatManager import InfoDatManager
from commons_test.MetaInfoHook import InterfaceInfo, SourceInfo
from commons_test.CollectHistory import CollectHistory
from commons_test.PathUtil import PathUtil, LocalPathInfos
from commons_test.SecurityClient import SecurityClient
from commons.ConvertToParquet import convert_sequentially
from commons_test.ConvertToParquet2_v2 import convert
from commons_test.CompressionManager import CompressionManager
from commons_test.CryptoClient import CryptoClient
from commons_test.CryptoExecutor import CryptoExecutor

logger = logging.getLogger(__name__)


class FtpHandler:

    def __init__(self, kafka_send):
        self.kafka_send = kafka_send
        self.info_dat_manager = InfoDatManager()
        self.delimiter_changer = DelimiterChanger()
        self.compression_manager = CompressionManager()
        self.is_header_skip = False

    def ftp_download(self, ftp: FTP, local_path_infos: LocalPathInfos, filename, coll_hist_dict, collect_history: CollectHistory, file_count, partitions, ulid, target_time, interface_info: InterfaceInfo,
                     source_info: SourceInfo = None):
        logger.info('fullPathDict: %s, interface_info: %s', local_path_infos, interface_info)
        local_tmp = local_path_infos.local_tmp_dir
        logger.debug("local_tmp : %s", local_tmp)
        try:
            self._check_external_process(interface_info, local_path_infos, filename, source_info)

            with open(local_path_infos.local_tmp_full_path, 'wb') as local_file:
                try:
                    startTime = datetime.now()
                    logger.info("FTP download started. target filenm : %s", filename)
                    ftp.retrbinary('RETR ' + filename, local_file.write)
                    endTime = datetime.now()
                    timeTaken = endTime - startTime
                    # get file from source -> local temp dir
                    logger.info("Download %s, Time taken : %s , Target time : %s", filename, timeTaken, target_time)
                    logger.info("local file size : %s", os.path.getsize(local_path_infos.local_tmp_full_path))

                    if source_info.file_proto_info.delete_source_after_download:
                        ftp.delete(filename)
                        logger.info(f"remove {filename} at source server {local_path_infos.source_file_path}")

                except Exception as e:
                    if local_path_infos.local_tmp_full_path is None:
                        raise Exception("Is not a Proper file...")
                    coll_hist_dict.err_msg = "filenm : {}, {}".format(filename, str(e)[:1024])
                    collect_history.set_status_code(coll_hist_dict, 2)
                    coll_hist_dict.ended_at = datetime.now().isoformat()
                    # send error message to kafka
                    self.kafka_send.send_to_kafka(coll_hist_dict)
                    raise Exception("Error FTP get, file : %s" % filename)

            self.is_header_skip = source_info.file_proto_info.header_skip

            self._check_xlsx(interface_info, local_path_infos, filename, coll_hist_dict, collect_history, partitions, ulid, file_count)
            # write info.dat file
            self._check_tar(source_info, filename, local_path_infos, coll_hist_dict, collect_history, partitions, ulid, file_count)

            # self._check_zip_file(interface_info, source_info, full_path_dict.get("tmpFullPath"), partitions, full_path_dict, collect_history, ulid)

            self._check_encrypt(interface_info, source_info, local_path_infos)

            self._check_delete_delimiter(source_info, local_path_infos, interface_info)

            self.delimiter_changer.replace(local_path_infos.local_tmp_full_path, interface_info)

            self._check_use_partition(interface_info, filename, local_path_infos)

            self._check_skip_header(interface_info, local_path_infos.local_tmp_full_path)

            self._check_to_write_infodat(interface_info, source_info, local_path_infos, partitions, ulid)
            self._convert_to_parquet(local_path_infos, source_info, interface_info, collect_history, partitions, ulid)

            logger.info("FTP download completed at %s", local_path_infos.local_tmp_full_path)
            file_count = file_count + 1
            return file_count
        except Exception as e:
            raise Exception("Failed to download file: %s" % filename)

    def ftp_get_tar_gzip(self, ftp: FTP, local_full_tmp, filename, collect_history):
        logger.info("tar gzip local path : %s", local_full_tmp)
        if not os.path.exists(local_full_tmp):
            with open(local_full_tmp, 'wb') as local_file:
                try:
                    start_time = datetime.now()
                    logger.info("tar gzip file download start, target filenm : %s", filename)
                    ftp.retrbinary('RETR ' + filename, local_file.write)
                    end_time = datetime.now()
                    time_taken = end_time - start_time
                    logger.info("tar gzip file download success, Time taken %s", time_taken)
                    return local_full_tmp
                except Exception as e:
                    self.kafka_send.sendErrorKafka(collect_history, 2, True,
                                                   "Error while getting tar gzip file" + str(e)[:1024])
                    raise Exception("failed to download file : %s" % filename)
        else:
            logger.info("%s file exist...", filename)
            return local_full_tmp

    def sftp_download(self, sftp: SFTPClient, local_path_infos: LocalPathInfos, filename, coll_hist_dict, collect_history: CollectHistory, file_count, partitions, ulid, target_time,
                      interface_info: InterfaceInfo, source_info: SourceInfo = None):
        logger.info('fullPathDict: %s, meta_info: %s, meta_info.ftp_server_info.data_source_dir: %s', local_path_infos,
                    interface_info, type(interface_info.ftp_proto_info.source_dir))
        local_tmp = local_path_infos.local_tmp_dir
        logger.debug("local_tmp : %s", local_tmp)
        try:

            self._check_external_process(interface_info, local_path_infos, filename, source_info)

            try:
                logger.info("Target filename : %s", filename)
                startTime = datetime.now()
                logger.info("SFTP download started. target filename : %s", filename)
                sftp.get(str(local_path_infos.source_file_path), str(local_path_infos.local_tmp_full_path))
                endTime = datetime.now()
                timeTaken = endTime - startTime
                logger.info("SFTP download success, filename : %s, Time taken : %s", filename, timeTaken)
                logger.info("local file size : %s", os.path.getsize(local_path_infos.local_tmp_full_path))

                if source_info.file_proto_info.delete_source_after_download:
                    sftp.remove(str(local_path_infos.source_file_path))
                    logger.info(f"remove {filename} at source server {local_path_infos.source_file_path}")
            except Exception as e:
                coll_hist_dict.err_msg = "filename : {}, {}".format(filename, str(e)[:1024])
                current_time = datetime.now().isoformat()
                coll_hist_dict.ended_at = current_time
                collect_history.set_status_code(coll_hist_dict, 2)
                # set Error info and send to kafka
                self.kafka_send.send_to_kafka(coll_hist_dict)
                raise Exception("Error SFTP get, file : %s" % filename)

            self.is_header_skip = source_info.file_proto_info.header_skip

            self._check_xlsx(interface_info, local_path_infos, filename, coll_hist_dict, collect_history, partitions, ulid, file_count)
            # write info.dat file
            self._check_tar(source_info, filename, local_path_infos, coll_hist_dict, collect_history, partitions, ulid, file_count)

            # self._check_zip_file(interface_info, source_info, full_path_dict.get("tmpFullPath"), partitions, full_path_dict, collect_history, ulid)

            self._check_encrypt(interface_info, source_info, local_path_infos)

            self._check_delete_delimiter(source_info, local_path_infos, interface_info)

            self.delimiter_changer.replace(local_path_infos.local_tmp_full_path, interface_info)

            self._check_use_partition(interface_info, filename, local_path_infos)

            self._check_skip_header(interface_info, local_path_infos.local_tmp_full_path)

            self._check_to_write_infodat(interface_info, source_info, local_path_infos, partitions, ulid)

            self._convert_to_parquet(local_path_infos, source_info, interface_info, collect_history, partitions, ulid)

            logger.info("FTP download completed at %s", local_tmp)
            file_count = file_count + 1
            return file_count

        except Exception as e:
            raise Exception("Failed to download file: %s" % filename)

    def sftp_get_tar_gzip(self, sftp: SFTPClient, local_path_infos: LocalPathInfos, filename, collect_history):
        try:
            startTime = datetime.now()
            local_full_tmp = PathUtil.get_zip_path(local_path_infos.local_tmp_full_path, filename)
            source_full_dir = PathUtil.get_zip_path(local_path_infos.source_file_path, filename)
            logger.info("SFTP download started. target filename : %s", filename)
            sftp.get(str(source_full_dir), str(local_full_tmp))
            endTime = datetime.now()
            timeTaken = endTime - startTime
            logger.info("Download success : %s, Time Taken: %s", filename, timeTaken)
            logger.info("tar gzip file download success")
            return local_full_tmp
        except Exception as e:
            self.kafka_send.sendErrorKafka(collect_history, 2, True, "Error while getting tar gzip file" + str(e)[:1024])
            raise Exception("failed to download zip file, file %s" % filename)

    def _convert_excel_csv(self, local_path_infos: LocalPathInfos, filename, chd, collect_history, partitions, ulid, interface_info: InterfaceInfo):
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

    def _extract_tar(self, source_info: SourceInfo, tar_full_path, chd, collect_history: CollectHistory, filename, partitions, local_path_infos: LocalPathInfos, ulid):
        local_tar_dir = Path(os.path.dirname(tar_full_path), "tar_tmp")
        os.makedirs(local_tar_dir, exist_ok=True)
        try:

            extracted_files = self.compression_manager.decompress_tar_file(tar_full_path, local_tar_dir)
            for extract_file in extracted_files:
                # if not source_info.file_proto_info.merge:
                #     self.info_dat_manager.write_info_dat(extract_file, partitions.get("hdfsPath"), os.path.basename(extract_file),
                #                                          full_path_dict.get("infoDatPath"), ulid)

                if source_info.file_proto_info.merge:
                    merge_extract_file = str(extract_file)+".merge"
                    self.info_dat_manager.write_info_dat(merge_extract_file, partitions.get("hdfsPath"), os.path.basename(extract_file),
                                                         local_path_infos.local_info_data_path, ulid)
                else:
                    self.info_dat_manager.write_info_dat(extract_file, partitions.get("hdfsPath"), os.path.basename(extract_file),
                                                         local_path_infos.local_info_data_path, ulid)
            return len(extracted_files)
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

    def _check_skip_header(self, interface_info: InterfaceInfo, local_full_path):
        if self.is_header_skip and not str(local_full_path).endswith(".zip"):
            if interface_info.ftp_proto_info.format == 'gz':
                compression_manager = CompressionManager()
                local_full_path = compression_manager.decompress_gz_file(local_full_path)
            tmp_file_path = str(local_full_path) + ".tmp"
            if os.path.getsize('local_full_path') > 0:
                with open(local_full_path, 'r', encoding=interface_info.ftp_proto_info.encode) as local_file:
                    next(local_file)
                    with open(tmp_file_path, 'w', encoding=interface_info.ftp_proto_info.encode) as tmp_file:
                        for line in local_file:
                            tmp_file.write(line)
                if os.path.exists(tmp_file_path):
                    os.rename(tmp_file_path, local_full_path)
                logger.info("header removed successfully..")

    def _execute_file_encryption(self, source_info: SourceInfo, local_path_infos: LocalPathInfos, interface_info: InterfaceInfo):
        try:
            crypto_executor = CryptoExecutor(self.is_header_skip, interface_info.ftp_proto_info.encode, interface_info.ftp_proto_info.delimiter,
                                             source_info.file_proto_info.crypto_rules)

            self.is_header_skip = crypto_executor.exec(local_path_infos.local_tmp_full_path)
        except Exception as e:
            raise

    def _crypto_file_content(self, interface_info : InterfaceInfo, local_path_infos: LocalPathInfos, column_num, crypto_type, crypto_client, tmp_file_path, is_encryption):
        buffer = []
        lines = []

        try:
            with open(local_path_infos.local_tmp_full_path, 'r', encoding=interface_info.ftp_proto_info.encode) as local_file:
                delimiter = bytes(interface_info.ftp_proto_info.delimiter, "utf-8").decode("unicode_escape")
                while True:
                    line = local_file.readline()
                    if self.is_header_skip:
                        self.is_header_skip = False
                    else:
                        if not line:
                            logger.info("No more line, finish to encrypt")
                            break

                        line_arr = line.split(delimiter)

                        if len(line_arr) < 2:
                            logger.info("No delimiter, Nothing to encrypt")
                            continue
                        try:
                            line_arr[column_num] = line_arr[column_num].rstrip()

                            buffer.append(line_arr[column_num])
                            lines.append(line_arr)

                            if len(buffer) >= 5000:
                                self._write_crypto_value(buffer, lines, crypto_type, column_num, delimiter, tmp_file_path, crypto_client, interface_info, is_encryption)
                                buffer.clear()
                                lines.clear()

                        except Exception as e:
                            logger.error(e)
                            break

            if len(buffer) >= 1:
                self._write_crypto_value(buffer, lines, crypto_type, column_num, delimiter, tmp_file_path, crypto_client, interface_info, is_encryption)
        except Exception as e:
            logger.error(f"Error during crypto : {e}")

    def _write_crypto_value(self, buffer, lines, crypto_type, column_num, delimiter, tmp_file_path, crypto_client: CryptoClient, interface_info: InterfaceInfo, is_encryption):
        write_values = []
        for crypto_data in buffer:
            try:
                if is_encryption:
                    write_values.append(crypto_client.enc(crypto_type, crypto_data))
                else:
                    write_values.append(crypto_client.dec(crypto_type, crypto_data))
            except Exception as e:
                logger.error(f"error during crypto : {e}, need to check origin file")
                write_values = []
                break
        for idx in range(len(write_values)):
            lines[idx][column_num] = write_values[idx]
            if not lines[idx][-1].endswith("\n"):
                write_line = delimiter.join(lines[idx]) + "\n"
            else:
                write_line = delimiter.join(lines[idx])
            write_values.append(write_line)

        if write_values:
            with open(tmp_file_path, 'a', encoding=interface_info.ftp_proto_info.encode) as tmp_file:
                tmp_file.writelines(write_values)
        else:
            raise Exception("No encrypt data to write.")

    def _delete_columns(self, source_info: SourceInfo, local_path_infos: LocalPathInfos, interface_info: InterfaceInfo):
        logger.info("delete column in particular index")
        tmp_file_path = str(local_path_infos.local_tmp_full_path) + ".tmp"
        delimiter = bytes(interface_info.ftp_proto_info.delimiter, "utf-8").decode("unicode_escape")
        lines = []
        with open(local_path_infos.local_tmp_full_path) as local_file:
            while True:
                line = local_file.readline()
                if not line:
                    break
                line_arr = line.split(delimiter)
                if len(line_arr) > 1:
                    lines.append(line_arr)
            self._delete_and_write(lines, tmp_file_path, source_info, delimiter)
        logger.info("completed to delete columns")
        if os.path.exists(tmp_file_path):
            os.rename(tmp_file_path, local_path_infos.local_tmp_full_path)

    def _delete_and_write(self, lines, tmp_file_path, source_info: SourceInfo, delimiter):
        delete_columns = source_info.file_proto_info.delete_columns
        logger.info("delete_columns value : %s", delete_columns)
        with open(tmp_file_path, 'w') as tmp_file:
            for line_arr in lines:
                for idx in sorted(delete_columns, reverse=True):
                    del line_arr[idx-1]
                logger.info("line_arr after deletion: %s", line_arr)
                write_line = delimiter.join(line_arr)
                logger.info("write line : %s", write_line)
                tmp_file.write(write_line)

    def _check_external_process(self, interface_info: InterfaceInfo, local_path_infos: LocalPathInfos, filename, source_info: SourceInfo):
        if interface_info.ftp_proto_info.external_process:
            local_path_infos.local_tmp_full_path = Path(interface_info.ftp_proto_info.ramdisk_dir, "in", filename)
        else:
            local_path_infos.local_tmp_full_path = self._add_dirpath_to_filenm(local_path_infos, filename, interface_info, source_info)

    def _check_xlsx(self, interface_info: InterfaceInfo, local_path_infos: LocalPathInfos, filename, coll_hist_dict, collect_history: CollectHistory, partitions, ulid, file_count):
        if interface_info.ftp_proto_info.format == "xlsx":
            self._convert_excel_csv(local_path_infos, filename, coll_hist_dict, collect_history, partitions, ulid, interface_info)
            file_count = file_count + 1
            return file_count

    def _check_tar(self, source_info: SourceInfo, filename, local_path_infos: LocalPathInfos, coll_hist_dict, collect_history: CollectHistory, partitions, ulid, file_count):
        if filename.endswith(".tar"):
            extracted_file_count = self._extract_tar(source_info, local_path_infos.local_tmp_full_path, coll_hist_dict, collect_history, filename,
                                                     partitions, local_path_infos, ulid)
            file_count = file_count + extracted_file_count
            return file_count

    def _check_encrypt(self, interface_info: InterfaceInfo, source_info: SourceInfo, local_path_infos: LocalPathInfos):
        self._execute_file_encryption(source_info, local_path_infos, interface_info)

    def _check_delete_delimiter(self, source_info: SourceInfo, local_path_infos: LocalPathInfos, interface_info: InterfaceInfo):
        if source_info and source_info.file_proto_info.delete_columns:
            self._delete_columns(source_info, local_path_infos, interface_info)

    def _check_use_partition(self, interface_info: InterfaceInfo, filename, local_path_infos: LocalPathInfos):
        if not interface_info.ftp_proto_info.use_partitions:
            filename = PathUtil.remove_date_filenm(filename)
        else:
            filename = os.path.basename(local_path_infos.local_tmp_full_path)

    def _check_to_write_infodat(self, interface_info: InterfaceInfo, source_info: SourceInfo, local_path_infos: LocalPathInfos, partitions, ulid):
        filename = os.path.basename(local_path_infos.local_tmp_full_path)
        # if (not interface_info.ftp_proto_info.external_process and not filename.endswith(".tar") and interface_info.ftp_proto_info.format != "xlsx"
        #         and not filename.endswith(".zip") and not source_info.file_proto_info.merge):
        if source_info.file_proto_info.merge and not filename.endswith(".tar"):
            local_path_infos.local_tmp_full_path = str(local_path_infos.local_tmp_full_path) + ".merge"
            logger.info("change local file name to %s", local_path_infos.local_tmp_full_path)

        if not interface_info.ftp_proto_info.external_process and not filename.endswith(".tar") and interface_info.ftp_proto_info.format != "xlsx":
                # and not filename.endswith(".zip")):
            self.info_dat_manager.write_info_dat(local_path_infos.local_tmp_full_path, partitions.get("hdfsPath"), filename, local_path_infos.local_info_data_path, ulid)

    def _convert_to_parquet(self, local_path_infos: LocalPathInfos, source_info: SourceInfo, interface_info: InterfaceInfo, collect_history: CollectHistory, partitions, ulid):
        try:
            # source_info.file_proto_info.convert_to = "parquet"
            if source_info.file_proto_info.convert_to == "parquet" and not source_info.file_proto_info.merge:
                logger.info("need to convert to parquet")
                if not source_info.file_proto_info.merge:
                    # TPDO-490
                    # if source_info.isMappedWithProvideTable:
                    #     logger.info("need to save origin file before convert")
                    #     local_file_path = full_path_dict.get("tmpFullPath")
                    #     info_dat_path = full_path_dict.get("infoDatPath")
                    #
                    #     coll_hist_dict = convert_to_parquet.save_origin_file_before_convert(local_file_path, info_dat_path, collect_history, partitions, ulid)
                    #     self.kafka_send.send_to_kafka(coll_hist_dict)
                    # parquet_files = convert(str(full_path_dict.get("tmpFullPath")), source_info, interface_info, collect_history, full_path_dict.get("infoDatPath"))
                    # for parquet_file in parquet_files:
                    #     collect_history.chdDict[ulid].local_path = parquet_file
                    #     collect_history.chdDict[ulid].filenm = os.path.basename(parquet_file)
                    #     self.kafka_send.send_to_kafka(collect_history.chdDict[ulid])
                    #     self.info_dat_manager.write_info_dat(parquet_file, partitions.get("hdfsPath"), os.path.basename(parquet_file), full_path_dict.get("infoDatPath"), ulid)
                    convert_sequentially(source_info, interface_info, local_path_infos.local_tmp_full_path, partitions.get("partitions"),
                                         collect_history, local_path_infos.local_info_data_path, source_info.file_proto_info.target_time)
        except Exception as e:
            raise Exception(e)

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

    def _write_origin_file_to_info_dat(self, interface_info, local_tmp_full_path, hdfs_path, info_dat_path, ulid):
        if (not interface_info.ftp_proto_info.external_process and not os.path.basename(local_tmp_full_path).endswith(".tar")
                and interface_info.ftp_proto_info.format != "xlsx"):
            self.info_dat_manager.write_info_dat(local_tmp_full_path, hdfs_path, os.path.basename(local_tmp_full_path),
                                                 info_dat_path, ulid)

    def _check_zip_file(self, interface_info: InterfaceInfo, source_info: SourceInfo, local_tmp_full_path, partitions,
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
            # local_tmp_path_pattern = os.path.dirname(local_tmp_full_path) + "/*"
            for extract_file in extract_file_list:
                extract_file_name = os.path.basename(extract_file)
                logger.info("sending .zip extract file kafka message")

                pti = partitions.get("partitions")
                ulid = collect_history.start_collect_h(source_info.collect_source_seq, extract_file,
                                                       partitions.get("hdfsPath"), extract_file_name,
                                                       source_info.file_proto_info.target_time, pti)
                coll_hist_dict = collect_history.chdDict[ulid]
                self.kafka_send.send_to_kafka(coll_hist_dict)

                self._check_skip_header(interface_info, extract_file)

                self.info_dat_manager.write_info_dat(extract_file, partitions.get("hdfsPath"), extract_file_name, local_path_infos.local_info_data_path, ulid)
