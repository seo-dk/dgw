import subprocess
import logging
import os
import re
import paramiko
from datetime import datetime

#from airflow.models import Variable



logger = logging.getLogger(__name__)
#logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))

class DistcpEncryptManager:
    def __init__(self, hdfs_path, enc_rules, format, target_compression):
        self.hdfs_path = hdfs_path
        self.enc_rules = enc_rules
        self.ssh = None
        self._init_ssh_client()
        self.format = format
        self.target_compression = target_compression

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
                'hostname': '90.90.43.101',
                'port': 22,
                'username': 'hadoop',
                'password': '!exodus10!',
                'look_for_keys': False,
                'allow_agent': False
            })
            self.channel = self.ssh.invoke_shell()
        except Exception as e:
            logger.exception("error connecting ssh")

    def close(self):
        if self.ssh:
            self.ssh.close()
            self.ssh = None

    def start(self):
        ## app 7번 방화벽 열리기 전 까지 이거 활용
        ciphers_keys = {'imsi': '636c64686b64686b', 'mdn': '676b73666b746b73', 'enb': '603f20dd57cd4747', 'svrcd': '776c73656874726f'}

        try:
            if __name__ == "__main__":
                pattern_input_path = os.path.join(self.hdfs_path, "00000*")
            else:
                pattern_input_path = os.path.join(self.hdfs_path, "00*")

            output_path = self.extract_tags_from_input(pattern_input_path)

            return self._exec(ciphers_keys, pattern_input_path, output_path)
        except Exception as e:
            raise Exception(f"An error occurred: {e}")

    def extract_tags_from_input(self, local_path):
        db_tb_dt_match = re.search(r'/db=([^/]+)/tb=([^/]+)/dt=([^/]+)', local_path)
        hh_match = re.search(r'/hh=([^/]+)', local_path)
        hm_match = re.search(r'/hm=([^/]+)', local_path)

        if not db_tb_dt_match:
            raise ValueError("Input path does not contain the expected dt/hh structure")
        else:
            db, tb, dt = db_tb_dt_match.group(1), db_tb_dt_match.group(2), db_tb_dt_match.group(3)

        if hh_match:
            hh = hh_match.group(1)
            output_path = f"/tap_d/test_dna_tr/out/db={db}/tb={tb}/dt={dt}/hh={hh}"
            return output_path
        if hm_match:
            hm = hm_match.group(1)
            output_path = f"/tap_d/test_dna_tr/out/db={db}/tb={tb}/dt={dt}/hm={hm}"
            return output_path
        output_path = f"/tap_d/test_dna_tr/out/db={db}/tb={tb}/dt={dt}"
        return output_path

    def _exec(self, cipher_keys, input_file, output_path):
        start_time = datetime.now()
        exec_command = f'cd /data1/datagw/encrypt && sh run.sh {input_file} {output_path} "{cipher_keys}" "{self.enc_rules}" {self.format} {self.target_compression}'
        logger.info("exec_command: %s", exec_command)

        _, stdout, stderr = self.ssh.exec_command(exec_command)
        logger.debug("start_spark_process")

        error_line = stderr.readlines()
        if error_line:
            logger.error("error: %s", error_line)
        else:
            logger.info(f"finished encryption, total process time : {datetime.now() - start_time}")
        return output_path
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
    #hdfs_path = "hdfs://dataGWCluster/staging/test/idcube_out/db=o_datalake/tb=pin/dt=20241206/hh=23"
    hdfs_path = "hdfs://90.90.43.21:8020/tap_d/test_dna_tr/db=tab_d/tb=dna_tr/dt=20241210/hh=13"
    ciphers_keys = {'imsi': '636c64686b64686b', 'mdn': '676b73666b746b73', 'enb': '603f20dd57cd4747', 'svrcd': '776c73656874726f'}
    enc_rules =   [{"type": "imsi", "col_pos": [1]}, {"type": "enb", "col_pos": [6,7,9,10]}]
    format = "csv"
    target_compression = "snappy"
    try:
        with DistcpEncryptManager(hdfs_path, enc_rules, format, target_compression) as disctcp_encrypt_manager:
            disctcp_encrypt_manager.start()
    except Exception as e:
        raise Exception(f"An error occurred: {e}")

