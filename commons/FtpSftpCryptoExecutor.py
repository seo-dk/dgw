from commons.MetaInfoHook import SourceInfo, InterfaceInfo
from commons.SecurityClient import SecurityClient
from commons.CryptoClient import CryptoClient
from commons.InfodatManager import InfoDatManager
from commons.HdfsUtil import put_files, make_directory

import logging
from datetime import datetime
import os
import shutil

logger = logging.getLogger(__name__)


class FtpSftpCryptoExecutor:
    def __init__(self, interface_info: InterfaceInfo, info_dat_manager: InfoDatManager, collected_file_info):
        self.interface_info = interface_info
        self.collected_file_info = collected_file_info

        self.source_info : SourceInfo = collected_file_info.source_info
        self.ulid = collected_file_info.ulid
        self.local_path_infos = collected_file_info.local_path_infos
        self.partitions = collected_file_info.partitions

        self.info_dat_manager = info_dat_manager

        xdr_client = SecurityClient()
        cipher_keys = xdr_client.get_keys()
        self.crypto_client = CryptoClient(cipher_keys)

        self.is_header_skip = self.source_info.file_proto_info.header_skip

    def start(self):
        if self.source_info.file_proto_info.enc_rules:
            logger.info("start encrypt")
            return self._start_encrypt_decrypt(self.source_info.file_proto_info.enc_rules, True)
        if self.source_info.file_proto_info.dec_rules:
            logger.info("start decrypt")
            return self._start_encrypt_decrypt(self.source_info.file_proto_info.dec_rules, False)


    def _start_encrypt_decrypt(self, crypto_rules, is_encrypt):
        try:
            store_raw_and_encrypted = next((crypto_rule for crypto_rule in crypto_rules if "store_raw_and_encrypted" in crypto_rule), None)
            logger.info(f"store_raw_and_encrypted : {store_raw_and_encrypted}")
            if store_raw_and_encrypted and store_raw_and_encrypted.get("store_raw_and_encrypted") == "none":
                return self.is_header_skip
            else:
                for crypto_rule in crypto_rules:
                    if "store_raw_and_encrypted" in crypto_rule and crypto_rule.get("store_raw_and_encrypted") == "both":
                        self._upload_raw_data_to_hadoop()
                        continue
                    crypto_type = crypto_rule["type"]
                    logger.info(f"crypto type: {crypto_type}")
                    crypto_columns = crypto_rule["col_pos"]

                    for column_num in crypto_columns:
                        logger.info("column: %s", column_num)
                        column_num -= 1

                        tmp_file_path = str(self.local_path_infos.local_tmp_full_path) + ".tmp"

                        if os.path.exists(tmp_file_path):
                            os.remove(tmp_file_path)

                        start_time = datetime.now()
                        tmp_full_path = self.local_path_infos.local_tmp_full_path
                        logger.info(f"{tmp_full_path} crypto start, start time : {start_time}")

                        self._log_line_count()

                        self._crypto_file_content(column_num, crypto_type, tmp_file_path, is_encrypt)

                        if os.path.exists(tmp_file_path):
                            self._log_line_count()
                            os.rename(tmp_file_path, self.local_path_infos.local_tmp_full_path)

                        end_time = datetime.now()
                        time_taken = end_time - start_time
                        logger.info("encrypted successfully ended, taken time : %s", time_taken)

                return self.is_header_skip
        except Exception as e:
            logger.exception("")
            raise

    def _upload_raw_data_to_hadoop(self):
        logger.info(f"local_tmp_full_path : {self.local_path_infos.local_tmp_full_path}")

        staging_hdfs_path = self.partitions.get("hdfsPath").replace("/idcube_out", "/staging/raw/idcube_out")
        staging_local_path = str(self.local_path_infos.local_tmp_full_path).strip().replace("/data/idcube_out", "/data/staging/raw/idcube_out")

        staging_dir = os.path.dirname(staging_local_path)
        if not os.path.exists(staging_dir):
            os.makedirs(staging_dir)
        logger.info(f"staging_local_path : {staging_local_path}")

        buffer_size = 64 * 1024 * 1024
        logger.info("copying raw file to staging path")
        with open(self.local_path_infos.local_tmp_full_path, encoding=self.interface_info.ftp_proto_info.encode) as collected_file:
            if self.is_header_skip:
                logger.info("header skipping during copying raw file")
                next(collected_file)
            with open(staging_local_path, 'w', encoding='utf-8') as staging_file:
                while True:
                    chunk = collected_file.read(buffer_size)
                    if not chunk:
                        break
                    staging_file.write(chunk)

        logger.info(f"staging_hdfs_path : {staging_hdfs_path}")
        make_directory(staging_hdfs_path)
        put_files(str(staging_local_path), str(staging_hdfs_path))

    def _log_line_count(self):
        with open(self.local_path_infos.local_tmp_full_path, 'r', encoding=self.interface_info.ftp_proto_info.encode) as file:
            before_line_count = sum(1 for _ in file)
        logger.info("line count : %s", before_line_count)

    def _crypto_file_content(self, column_num, crypto_type, tmp_file_path, is_encrypt):
        buffer = []
        lines = []
        try:
            with open(self.local_path_infos.local_tmp_full_path, 'r', encoding=self.interface_info.ftp_proto_info.encode) as local_file:
                delimiter = bytes(self.interface_info.ftp_proto_info.delimiter, "utf-8").decode("unicode_escape")
                while True:
                    line = local_file.readline()
                    if self.is_header_skip:
                        logger.info("header skipping at crypto")
                        self.is_header_skip = False
                    else:
                        if not line:
                            logger.info("No more line, finish to crypto")
                            break

                        line_arr = line.split(delimiter)

                        if len(line_arr) < 2:
                            logger.info("No delimiter, Nothing to encrypt/decrypt")
                            continue
                        line_arr[column_num] = line_arr[column_num].rstrip()

                        buffer.append(line_arr[column_num])
                        lines.append(line_arr)

                        if len(buffer) >= 5000:
                            self._write_crypto_value(buffer, lines, crypto_type, column_num, delimiter, tmp_file_path, is_encrypt)

                            buffer.clear()
                            lines.clear()
            if len(buffer) >= 1:
                self._write_crypto_value(buffer, lines, crypto_type, column_num, delimiter, tmp_file_path, is_encrypt)

        except Exception as e:
            logger.exception("Error during crypto")

    def _write_crypto_value(self, buffer, lines, crypto_type, column_num, delimiter, tmp_file_path, is_encrypt):
        crypto_values = []

        for crypto_data in buffer:
            try:
                if is_encrypt:
                    crypto_values.append(self.crypto_client.enc(crypto_type, crypto_data))
                else:
                    crypto_values.append(self.crypto_client.dec(crypto_type, crypto_data))
            except Exception as e:
                logger.exception(f"Error at CryptoClient.enc()/CryptoClient.enc()dec, data : {crypto_data}")
                raise

        for idx, data in enumerate(crypto_values):
            lines[idx][column_num] = data
            if not lines[idx][-1].endswith("\n"):
                lines[idx] = delimiter.join(lines[idx]) + "\n"
            else:
                lines[idx] = delimiter.join(lines[idx])

        with open(tmp_file_path, 'a', encoding=self.interface_info.ftp_proto_info.encode) as tmp_file:
            tmp_file.writelines(lines)

