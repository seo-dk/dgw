import logging
import os.path
from datetime import datetime
import re

from airflow.models import Variable
from typing import Literal
import paramiko

from commons_test.HdfsUtil import remove_files, is_file_path, remove_diretory
from commons_test.MetaInfoHook import SparkServerInfo

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))

class RemoveFileManager:
    def __init__(self, remove_file_path, mode: Literal['local', 'hadoop', 'remote']):
        self.remove_file_path = remove_file_path
        self.mode = mode

    def start(self, server_info: SparkServerInfo = None):
        self._cut_to_dt_partition()
        if self.mode == "local":
            self._delete_local()
        elif self.mode == "hadoop":
            self._delete_hadoop()
        elif self.mode == "remote":
            if server_info:
                self._delete_remote_hadoop(server_info)
    def _delete_hadoop(self):
        try:
            if remove_diretory(self.remove_file_path):
                logger.info(f"{self.remove_file_path} remove success in hadoop")
            else:
                logger.info(f"{self.remove_file_path} remove fail")
        except Exception as e:
            logger.exception(f"Error during removing file...{self.remove_file_path}")
            raise Exception(e)

    def _delete_local(self):
        try:
            if os.path.exists(self.remove_file_path):
                os.remove(self.remove_file_path)
                logger.info(f"{self.remove_file_path} remove success in local")
        except Exception as e:
            logger.exception(f"Error during removing file...{self.remove_file_path}")
            raise Exception(e)

    def _delete_remote_hadoop(self, server_info: SparkServerInfo):
        logger.info(f"start removing at remote server, path : {self.remove_file_path}")
        ssh = None
        try:
            start_time = datetime.now()
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(**{
                'hostname': server_info.ip,
                'port': server_info.port,
                'username': server_info.username,
                'look_for_keys': True,
                'allow_agent': True
            })
            channel = ssh.invoke_shell()
            cmd = f"/bin/bash -l -c 'hdfs dfs -rm -r -f -skipTrash {self.remove_file_path}'"

            _, stdout, stderr = ssh.exec_command(cmd)

            exit_status, error_line = self._get_log_from_stdout(stdout, stderr)
            if exit_status == 0:
                logger.info(f"finished delete, total process time : {datetime.now() - start_time}")
            else:
                logger.error(f"error : {error_line}")
        except Exception as e:
            logger.exception("error connecting ssh")
        finally:
            if ssh:
                ssh.close()

    def _get_log_from_stdout(self, stdout, stderr):
        error_line = stderr.read().decode().strip()
        channel = stdout.channel
        exit_status = channel.recv_exit_status()
        logger.info("exit_status : %s", exit_status)
        return exit_status, error_line

    def _cut_to_dt_partition(self):
        match = re.search(r"(.*?/dt=\d{8})(?:/|$)", self.remove_file_path)
        if match:
            self.remove_file_path = str(match.group(1))
        else:
            raise Exception("There is no dt partition")