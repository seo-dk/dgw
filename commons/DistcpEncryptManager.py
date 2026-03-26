import json
import logging
import os
import re
import paramiko
from datetime import datetime
import base64
from dataclasses import dataclass, asdict
from typing import Dict, List

from airflow.models import Variable
import commons.HdfsUtil as HdfsUtil
from commons.MetaInfoHook import SparkServerInfo, ColumnCryptoInfo
from commons.VaultClient import get_vault_token, VAULT_IP, VAULT_ADDR, VAULT_NAMESPACE, VAULT_SECRET_PATH
from commons.SchemaInfoManager import SchemaInfoManager



logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))

@dataclass
class CryptoConfig:
    crypto_type: str = None
    enc_memory_usage: str = None
    file_nm_pattern: str = None
    staging_path: str = None
    column_crypto_infos: List[ColumnCryptoInfo] = None
    format: str = "csv"
    target_compression: str = None
    delimiter: str = None
    encoding: str = "UTF-8"
    encrypt_server_info: SparkServerInfo = SparkServerInfo()
    target_format: str = "csv"
    merge: bool = False

    @staticmethod
    def seriallize(column_crypto_infos: List[ColumnCryptoInfo]) -> List[dict]:
        if column_crypto_infos is None:
            return []
        return [{"type": info.type, "col_pos": info.col_pos} for info in column_crypto_infos]


class DistcpEncryptManager:
    def __init__(self, crypto_config: CryptoConfig, hdfs_dir, is_encrypted, store_raw_and_crypto, output_path=None):

        self.crypto_config = crypto_config
        self.hdfs_dir = hdfs_dir
        self.ssh = None
        self.tb = None
        self._init_ssh_client()
        self.app_id = None
        self.output_path = output_path
        self.is_encrypted = is_encrypted
        self.store_raw_and_crypto = store_raw_and_crypto

    def __enter__(self):
        if self.ssh is None:
            self._init_ssh_client()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _init_ssh_client(self):
        try:
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh.connect(**{
                'hostname': self.crypto_config.encrypt_server_info.ip,
                'port': self.crypto_config.encrypt_server_info.port,
                'username': self.crypto_config.encrypt_server_info.username,
                'look_for_keys': True,
                'allow_agent': True
            })
            self.channel = self.ssh.invoke_shell()
        except Exception as e:
            logger.exception("error connecting ssh")

    def close(self):
        if self.ssh:
            self.ssh.close()
            self.ssh = None

    def start(self):
        vault_token = get_vault_token()
        logger.debug("vault_token acquired")
        try:
            if __name__ == "__main__":
                pattern_input_path = os.path.join(self.crypto_config.staging_path, "*.snappy.parquet")
            else:
                pattern_input_path = self._check_file_path()
            if not self.output_path:
                self.output_path = self.extract_tags_from_input(pattern_input_path)
            file_exist = self._check_crypto_file_exist(pattern_input_path)

            return self._exec(vault_token, pattern_input_path, self.output_path, file_exist), pattern_input_path
        except Exception as e:
            raise Exception(f"An error occurred: {e}")

    def _check_crypto_file_exist(self, input_file_pattern):
        file_exist = False
        try:
            need_encrypt_files = HdfsUtil.get_file_list_with_ssh(self.ssh, input_file_pattern)
            if need_encrypt_files:
                file_exist = True
        except Exception as e:
            logger.warning(f"Error : {e}")
            logger.info(f"{input_file_pattern} is empty..")
        finally:
            return file_exist

    def _check_test_mode(self):
        pattern_input_path = os.path.join(self.crypto_config.staging_path, self.crypto_config.file_nm_pattern)
        logger.info(f"pattern_input_path : {pattern_input_path}")
        return pattern_input_path

    def extract_tags_from_input(self, local_path):

        def ensure_output_path(output_path):
            if self.is_encrypted and self.store_raw_and_crypto:
                return output_path.replace("/idcube_out", "/staging/raw/idcube_out")
            return output_path

        db_tb_dt_match = re.search(r'/db=([^/]+)/tb=([^/]+)/dt=([^/]+)', local_path)
        hh_match = re.search(r'/hh=([^/]+)', local_path)
        hm_match = re.search(r'/hm=([^/]+)', local_path)

        if not db_tb_dt_match:
            raise ValueError("Input path does not contain the expected dt/hh structure")
        else:
            db, tb, dt = db_tb_dt_match.group(1), db_tb_dt_match.group(2), db_tb_dt_match.group(3)
            self.tb = tb

        if hh_match:
            hh = hh_match.group(1)
            output_path = f"/idcube_out/db={db}/tb={tb}/dt={dt}/hh={hh}"
            return ensure_output_path(output_path)
        if hm_match:
            hm = hm_match.group(1)
            output_path = f"/idcube_out/db={db}/tb={tb}/dt={dt}/hm={hm}"
            return ensure_output_path(output_path)
        output_path = f"/idcube_out/db={db}/tb={tb}/dt={dt}"
        return ensure_output_path(output_path)

    def _encode_to_base64(self, context):
        json_string = json.dumps(context)
        encoded_result = base64.b64encode(json_string.encode()).decode()
        return encoded_result

    def _exec(self, vault_token, input_file_pattern, output_path, file_exist):
        start_time = datetime.now()
        if file_exist:
            encoded_crypto_rules, encoded_crypto_config, encoded_schema = self._set_command_argument()

            crypto_config_dict = json.loads(base64.b64decode(encoded_crypto_config).decode())
            crypto_config_dict.get("encrypt_server_info", {}).pop("username", None)
            crypto_config_dict.get("encrypt_server_info", {}).pop("password", None)
            encoded_crypto_config = self._encode_to_base64(crypto_config_dict)

            logger.info(f"exec file path : {self.crypto_config.encrypt_server_info.exec_file_path}")
            logger.info(f"encrtypt output_path : {output_path}")
            exec_command = (f'cd "{self.crypto_config.encrypt_server_info.exec_file_path}"\n'
                            f'export VAULT_TOKEN="{vault_token}"\n'
                            f'export VAULT_IP="{VAULT_IP}"\n'
                            f'export VAULT_ADDR="{VAULT_ADDR}"\n'
                            f'export VAULT_NAMESPACE="{VAULT_NAMESPACE}"\n'
                            f'export VAULT_SECRET_PATH="{VAULT_SECRET_PATH}"\n'
                            f'export ENCRYPT_CONFIG="{encoded_crypto_config}"\n'
                            f'export ENCODED_SCHEMA="{encoded_schema}"\n'
                            f'export ENC_RULES="{encoded_crypto_rules}"\n'
                            f'sh run_data_processing.sh "{input_file_pattern}" "{output_path}" "{self.tb}"')

            logger.debug("exec_command: %s", exec_command)

            logger.debug("start_spark_process")
            _, stdout, stderr = self.ssh.exec_command(exec_command)

            exit_status, url_match, error_line = self._get_log_from_stdout(stdout, stderr)

            if exit_status == 0:
                logger.info(f"finished encryption, total process time : {datetime.now() - start_time}")
                return output_path
            else:
                logger.error(f"error : {error_line}")
                if url_match:
                    logger.info(f"log url : {url_match.group(0)}")
                raise Exception(f"Error occur during encryption, application_id : {self.app_id}, log url: {url_match.group((0))}")
        else:
            HdfsUtil.make_directory(output_path)
            logger.info(f"created empty dir, empty_dir path : {output_path}")
            return output_path

    def _set_command_argument(self):
        encoded_crypto_infos = self._encode_to_base64(self.crypto_config.seriallize(self.crypto_config.column_crypto_infos))
        encoded_crypto_config = self._encode_to_base64(asdict(self.crypto_config))
        schema_info = None
        encoded_schema_info = self._encode_to_base64(schema_info)
        if self.crypto_config.format is None:
            logger.info("format is None")
        elif self.crypto_config.format not in ['orc', 'parquet'] and self.crypto_config.target_format in ['parquet', 'orc']:
            ## need to distcp to target server at here
            schema_info = SchemaInfoManager(self.hdfs_dir).get_schema_dict()
            encoded_schema_info = self._encode_to_base64(schema_info)

        return encoded_crypto_infos, encoded_crypto_config, encoded_schema_info

    def _get_log_from_stdout(self, stdout, stderr):
        output_line = stdout.read().decode().strip()
        error_line = stderr.read().decode().strip()
        channel = stdout.channel
        exit_status = channel.recv_exit_status()
        logger.info("output : %s", output_line)
        logger.info("exit_status : %s", exit_status)

        app_id_match = re.search('application_\d+_\d+', output_line)
        url_match = re.search('http://[^\s]+', output_line)
        if app_id_match:
            self.app_id = app_id_match.group(0)
            logger.info(f"app_id : {self.app_id}")
        else:
            logger.info(f"no application id was returned.")
        return exit_status, url_match, error_line


    def _check_file_path(self):
        if HdfsUtil.is_dir_check_with_ssh(self.ssh, self.crypto_config.staging_path):
            pattern_input_path = os.path.join(self.crypto_config.staging_path, self.crypto_config.file_nm_pattern)
            logger.info(f"pattern_input_path : {pattern_input_path}")
            return str(pattern_input_path)
        elif HdfsUtil.is_file_check_with_ssh(self.ssh, self.crypto_config.staging_path):
            logger.info(f"pattern_input_path : {self.crypto_config.staging_path}")
            return str(self.crypto_config.staging_path)

def setup_logger(log_file="encrypt.log"):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(log_file, mode='w')
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger


if __name__ == "__main__":
    logger = setup_logger()
    hdfs_path = "hdfs://dataGWCluster/staging/test/idcube_out/db=o_datalake/tb=td_zprd_prod_prod_mgmt_itm_val/dt=20250207/"
    enc_rules: List[ColumnCryptoInfo]
    column_crypto_info = ColumnCryptoInfo(type="mdn",col_pos= [2])
    enc_rules = [column_crypto_info]
    format = "parquet"
    target_compression = "snappy"
    source_file_pattern = "*.snappy.parquet"
    enc_memory_usage = "25g"
    try:

        crypto_type: str = None
        enc_memory_usage: str = None
        file_nm_pattern: str = None
        staging_path: str = None
        column_crypto_infos: List[ColumnCryptoInfo] = None
        format: str = "csv"
        target_compression: str = None
        delimiter: str = None
        encoding: str = "UTF-8"
        encrypt_server_info: SparkServerInfo = SparkServerInfo()
        target_format: str = "csv"

        encrypt_config = CryptoConfig("encryption", enc_memory_usage, source_file_pattern, hdfs_path, enc_rules, format, target_compression)
        with DistcpEncryptManager(encrypt_config) as disctcp_encrypt_manager:
            disctcp_encrypt_manager.start()
    except Exception as e:
        raise Exception(f"An error occurred: {e}")
