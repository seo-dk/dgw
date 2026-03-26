import glob
import logging
import os
import json
import pathlib
import subprocess
from datetime import datetime
import time
import os
import glob
import shutil

from pyarrow.fs import HadoopFileSystem, LocalFileSystem, FileType
from urllib.parse import urlparse
import pyarrow as pa
import paramiko

from airflow.exceptions import AirflowException
from airflow.models import Variable

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))

hadoop_home = '/local/HADOOP'

pattern = [hadoop_home + '/share/hadoop/' + d + '/**/*.jar' for d in ['hdfs', 'common']]
hdfs_cp = ':'.join(file for p in pattern for file in glob.glob(p, recursive=True))

os.environ['CLASSPATH'] = ':'.join([hadoop_home + '/etc/hadoop:', hdfs_cp])
os.environ['LD_LIBRARY_PATH'] = os.getenv('LD_LIBRARY_PATH', '') + ':' + hadoop_home + '/lib/native:'


class ParquetValidator:
    def __init__(self, port=8020, user='hadoop'):
        host = "default"
        self._hdfs = HadoopFileSystem(host, port=port, user=user)
        self.local_fs = LocalFileSystem()
        self.ssh = None
        self._init_ssh_client()


    def __enter__(self):
        if self.ssh is None:
            self._init_ssh_client()
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


    def _init_ssh_client(self):
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(**{
            'hostname': '90.90.47.63',
            'port': 22,
            'username': 'hadoop',
            'look_for_keys': True,
            'allow_agent': True
        })


    def close(self):
        if self.ssh:
            self.ssh.close()
            self.ssh = None


    def check_if_parquet(self, directory_uri):
        logger.info(f"Checking directory: {directory_uri}")
        try:
            parsed_uri = urlparse(directory_uri)
            directory_path = os.path.join(parsed_uri.netloc, parsed_uri.path)
            logger.info(f"Extracted path: {directory_path}")

            info = self._hdfs.get_file_info(directory_path)
            logger.info(f"Directory info: {info}, info.type: {info.type}")

            if info.type == pa.fs.FileType.Directory:
                selector = pa.fs.FileSelector(directory_path)
                file_infos = self._hdfs.get_file_info(selector)
                logger.info(f"file_infos: {file_infos}")
                for file_info in file_infos:
                    if file_info.path.endswith('.parquet'):
                        logger.info(f"Found parquet file: {file_info.path}")
                        return True
            return False
        except Exception as e:
            logger.exception("Failed to check if the directory contains parquet files")
            return False


    def validate_parquet(self, hdfs_path, interface_id):
        command = f'source ~/.bash_profile; python3 /home/hadoop/bin/is_parquet.py {hdfs_path} {interface_id}'
        logger.info("exec_command: %s", command)
        _, stdout, _ = self.ssh.exec_command(command)

        stdout_lines = stdout.readlines()
        json_output = json.loads("".join(stdout_lines))
        logger.info(f'parquet_format validated. json_output: {json_output}')
        stdout.channel.set_combine_stderr(True)
        return json_output['code'] == 1


if __name__ == "__main__":
    ParquetValidator = ParquetValidator()
    ParquetValidator.check_if_parquet("hdfs://dataGWCluster/idcube_out/db=o_common_raw/tb=bin_10by10_daily/dt=20240507")