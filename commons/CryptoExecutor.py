import logging
import os.path
from datetime import datetime
import shutil

from airflow.exceptions import AirflowException

from commons.SecurityClient import SecurityClient
from commons.CryptoClient import CryptoClient
from commons.MetaInfoHook import CryptoRules, ColumnCryptoInfo
from commons.HdfsUtil import make_directory, put_files

logger = logging.getLogger(__name__)

class CryptoExecutor:
    def __init__(self, is_header_skip, encode, delimiter, crypto_rules: CryptoRules):
        xdr_client = SecurityClient()
        cipher_keys = xdr_client.get_keys()
        self.crypto_client = CryptoClient(cipher_keys)
        self.is_header_skip = is_header_skip
        self.encode = encode
        self.delimiter = delimiter
        self.is_encrypt = True
        self.crypto_rules = crypto_rules

    def exec(self, result_file_path):
        if self.crypto_rules.store_raw_and_crypto and self.crypto_rules.enabled:
            self._upload_raw_data_to_hadoop(result_file_path)
        elif self.crypto_rules.enabled:
            self._start_encrypt_and_decrypt(result_file_path)

    def _start_encrypt_and_decrypt(self, result_file_path, is_raw=False):
        for crypto_type, column_crypto_infos in self.crypto_rules.rules.items():
            logger.info(f"crypto_type : {crypto_type}")
            is_encrypt = True
            if crypto_type == "decryption":
                logger.info("decryption start")
                is_encrypt = False
            else:
                logger.info("encryption start")

            for column_crypto_info in column_crypto_infos:
                logger.info(f"column_crypto_info : {column_crypto_info}")
                column_crypto_info: ColumnCryptoInfo
                logger.info(f"crypto_column_type : {column_crypto_info.type}")
                for column_num in column_crypto_info.col_pos:
                    logger.info("column: %s", column_num)
                    column_num -= 1

                    tmp_file_path = str(result_file_path) + ".tmp"

                    if os.path.exists(tmp_file_path):
                        os.remove(tmp_file_path)

                    start_time = datetime.now()
                    logger.info(f"{result_file_path} crypto start, start time : {start_time}")

                    self._log_line_count(result_file_path)

                    self._crypto_file_content(result_file_path, column_num, column_crypto_info.type, tmp_file_path, is_encrypt, is_raw)

                    if os.path.exists(tmp_file_path):
                        self._log_line_count(result_file_path)
                        os.rename(tmp_file_path, result_file_path)

                    end_time = datetime.now()
                    time_taken = end_time - start_time

                    logger.info("crypto successfully ended, taken time : %s", time_taken)

    def _crypto_file_content(self, src_path, column_num, column_crypto_type, crypto_file_path, is_encrypt, is_raw=False):
        buffer = []
        lines = []
        try:
            with open(src_path, 'r', encoding=self.encode) as local_file:
                delimiter = bytes(self.delimiter, "utf-8").decode("unicode_escape")
                try:
                    if self.is_header_skip:
                        logger.info("header skipping at crypto")
                        next(local_file)
                except StopIteration:
                    logger.info(f"{src_path} is empty file")
                while True:
                    line = local_file.readline()
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
                        self._write_crypto_value(buffer, lines, column_crypto_type, column_num, delimiter, crypto_file_path, is_encrypt, is_raw)

                        buffer.clear()
                        lines.clear()
            if len(buffer) >= 1:
                self._write_crypto_value(buffer, lines, column_crypto_type, column_num, delimiter, crypto_file_path, is_encrypt)
        except Exception as e:
            logger.exception("Error during crypto")

    def _write_crypto_value(self, buffer, lines, column_crypto_type, column_num, delimiter, crypto_file_path, is_encrypt, is_raw=False):
        crypto_values = []
        if is_raw:
            encoding = "utf-8"
        else:
            encoding = self.encode

        for crypto_data in buffer:
            try:
                if is_encrypt:
                    crypto_values.append(self.crypto_client.enc(column_crypto_type, crypto_data))
                else:
                    crypto_values.append(self.crypto_client.dec(column_crypto_type, crypto_data))
            except Exception as e:
                logger.exception(f"Error at CryptoClient.enc()/CryptoClient.enc()dec, data : {crypto_data}")
                raise

        for idx, data in enumerate(crypto_values):
            lines[idx][column_num] = data
            if not lines[idx][-1].endswith("\n"):
                lines[idx] = delimiter.join(lines[idx]) + "\n"
            else:
                lines[idx] = delimiter.join(lines[idx])

        with open(crypto_file_path, 'a', encoding=encoding) as tmp_file:
            tmp_file.writelines(lines)

    def _log_line_count(self, result_file_path):
        with open(result_file_path, 'r', encoding=self.encode) as file:
            before_line_count = sum(1 for _ in file)
        logger.info("line count : %s", before_line_count)

    def _upload_raw_data_to_hadoop(self, result_file_full_path):
        logger.info(f"local_tmp_full_path : {result_file_full_path}")

        result_file_dir = os.path.dirname(result_file_full_path)

        staging_hdfs_path = result_file_dir.replace("/data/idcube_out", "/staging/raw/idcube_out")
        staging_local_path = str(result_file_full_path).strip().replace("/data/idcube_out", "/data/staging/raw/idcube_out")

        logger.info(f"result_file_dir : {result_file_dir}")
        logger.info(f"staging_hdfs_path : {staging_hdfs_path}")
        logger.info(f"staging_local_path : {staging_local_path}")

        # if origin file is encrypted
        if self.crypto_rules.is_encrypted:
            ## copy origin file to staging path
            logger.info(f"copying {result_file_full_path} -> {staging_local_path}")
            os.makedirs(os.path.dirname(staging_local_path), exist_ok=True)
            shutil.copy(result_file_full_path, staging_local_path)

            ## decrypt a origin file which is at staging path and not remove header
            self._start_encrypt_and_decrypt(staging_local_path, True)
        else:
            staging_dir = os.path.dirname(staging_local_path)
            if not os.path.exists(staging_dir):
                os.makedirs(staging_dir)

            # copy raw file to staging path
            logger.info(f"staging_local_path : {staging_local_path}")
            logger.info("copying raw file to staging path")
            self._copy_to_staging_path(result_file_full_path, staging_local_path)

            # encrypt origin file.
            self._start_encrypt_and_decrypt(result_file_full_path)

        ## upload staging file to hdfs_path
        self._upload_to_hadoop(staging_local_path, staging_hdfs_path)

    def _copy_to_staging_path(self, src_file_path, result_file_path):
        buffer_size = 64 * 1024 * 1024

        with open(src_file_path, encoding=self.encode) as collected_file:
            if self.is_header_skip:
                logger.info("header skipping during copying raw file")
                next(collected_file)
            with open(result_file_path, 'w', encoding='utf-8') as staging_file:
                while True:
                    chunk = collected_file.read(buffer_size)
                    if not chunk:
                        break
                    staging_file.write(chunk)

    def _upload_to_hadoop(self,staging_local_path, staging_hdfs_path):
        ## upload staging file to hdfs_path
        logger.info(f"staging_hdfs_path : {staging_hdfs_path}")
        make_directory(staging_hdfs_path)
        put_files(str(staging_local_path), str(staging_hdfs_path))
