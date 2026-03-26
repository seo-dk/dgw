import json
import logging
import base64
import paramiko
from airflow.models import Variable
from commons.VaultClient import get_vault_token, VAULT_IP, VAULT_ADDR, VAULT_NAMESPACE, VAULT_SECRET_PATH

logger = logging.getLogger(__name__)

INPUT_PATH = "hdfs://90.90.43.21:8020/tap_d/staging/db=o_datalake/tb=pin/dt=20260310/hh=15/0*"
OUTPUT_PATH = "hdfs://90.90.43.21:8020/tap_d/db=o_datalake/tb=pin/dt=20260310/hh=15/"
TB_NAME = "pin"
ENC_RULES = [
    {"type": "imsi", "col_pos": [7]},
    {"type": "svrcd", "col_pos": [8]},
    {"type": "enb", "col_pos": [21, 78, 79, 80, 81, 82, 85, 88, 89, 90, 91, 92, 94, 95, 96, 97]},
]
ENCRYPT_CONFIG = {
    "crypto_type": "encryption",
    "enc_memory_usage": "25g",
    "file_nm_pattern": "0*",
    "staging_path": "hdfs://90.90.43.21:8020/tap_d/staging/db=o_datalake/tb=pin/dt=20260310/hh=15/",
    "column_crypto_infos": [
        {"type": "imsi", "col_pos": [7]},
        {"type": "svrcd", "col_pos": [8]},
        {"type": "enb", "col_pos": [21, 78, 79, 80, 81, 82, 85, 88, 89, 90, 91, 92, 94, 95, 96, 97]},
    ],
    "format": "orc",
    "target_compression": "snappy",
    "delimiter": "\u001e",
    "encoding": "UTF-8",
    "encrypt_server_info": {
        "ip": "90.90.43.101",
        "port": 22,
        "queue": "default",
        "exec_file_path": "/data1/datagw/encrypt",
        "hadoop_schema_path": "hdfs://dataGWCluster/schema",
        "username": "datagw",
        "password": "datagw",
        "is_external": True,
        "memory_usage": "25g",
    },
    "target_format": "orc",
    "merge": False,
}
ENCODED_SCHEMA = None


def _encode_to_base64(obj):
    return base64.b64encode(json.dumps(obj).encode()).decode()


class DistcpEncryptManagerTest:
    def __init__(self, host, port, username):
        self.host = host
        self.port = port
        self.username = username
        self.ssh = None

    def __enter__(self):
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(
            hostname=self.host,
            port=self.port,
            username=self.username,
            look_for_keys=True,
            allow_agent=True,
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.ssh:
            self.ssh.close()

    def start(self):
        vault_token = get_vault_token()
        encoded_enc_rules = _encode_to_base64(ENC_RULES)
        encrypt_config = dict(ENCRYPT_CONFIG)
        encrypt_config.get("encrypt_server_info", {}).pop("username", None)
        encrypt_config.get("encrypt_server_info", {}).pop("password", None)
        encoded_encrypt_config = _encode_to_base64(encrypt_config)
        encoded_schema = _encode_to_base64(ENCODED_SCHEMA)
        exec_command = (
            f'cd /data1/datagw/encrypt/\n'
            f'export VAULT_TOKEN="{vault_token}"\n'
            f'export VAULT_IP="{VAULT_IP}"\n'
            f'export VAULT_ADDR="{VAULT_ADDR}"\n'
            f'export VAULT_NAMESPACE="{VAULT_NAMESPACE}"\n'
            f'export VAULT_SECRET_PATH="{VAULT_SECRET_PATH}"\n'
            f'export ENCRYPT_CONFIG="{encoded_encrypt_config}"\n'
            f'export ENCODED_SCHEMA="{encoded_schema}"\n'
            f'export ENC_RULES="{encoded_enc_rules}"\n'
            f'sh /data1/datagw/encrypt/run_data_processing.sh'
            f' "{INPUT_PATH}"'
            f' "{OUTPUT_PATH}"'
            f' "{TB_NAME}"'
        )
        logger.info(f"exec_command: {exec_command}")
        _, stdout, stderr = self.ssh.exec_command(exec_command)
        output = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        exit_status = stdout.channel.recv_exit_status()
        logger.info(f"output: {output}")
        logger.info(f"exit_status: {exit_status}")
        if error:
            logger.warning(f"stderr: {error}")
        if exit_status != 0:
            raise Exception(f"run.sh failed with exit_status={exit_status}, stderr={error}")
        return output
