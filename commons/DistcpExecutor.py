import json
import logging
from enum import Enum

import paramiko
from airflow.models import Variable
from dataclasses import dataclass

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))


class DistcpStatus(Enum):
    SUCCESS = 4
    DATA_INEQUALITY = 5


@dataclass
class DistcpConfig:
    hdfs_source_path: str = None
    hdfs_dest_path: str = None
    interface_id: str = None
    skip_verify_count: int = 0
    clear_path: int = 0
    clear_dirs: int = 0


class DistcpExecutor:

    def __init__(self):
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

    def exec(self, distcp_exec_param: DistcpConfig):
        logger.info("skip_verify_count: %s, clear_path: %s", distcp_exec_param.skip_verify_count, distcp_exec_param.clear_path)
        command = (f'source ~/.bash_profile; python3 /home/hadoop/bin/distcp3.py "{distcp_exec_param.hdfs_source_path}" "{distcp_exec_param.hdfs_dest_path}" '
                   f'"{distcp_exec_param.interface_id}" "{distcp_exec_param.skip_verify_count}" "{distcp_exec_param.clear_path}" "{distcp_exec_param.clear_dirs}"')
        logger.info("exec_command: %s", command)
        _, stdout, _ = self.ssh.exec_command(command)

        stdout_lines = stdout.readlines()
        json_output = json.loads("".join(stdout_lines))
        logger.info("json_output: %s", json_output)
        stdout.channel.set_combine_stderr(True)
        return json_output

    def is_path_exist(self, hdfs_dest_path):
        command = f'source ~/.bash_profile; hdfs dfs -ls {hdfs_dest_path}'
        logger.info("exec_command: %s", command)
        stdin, stdout, stderr = self.ssh.exec_command(command)

        flag_exist = not bool(stderr.read().decode("utf-8"))
        logger.info("is_path_exist: %s", flag_exist)

        return flag_exist

    def exec_rename_file(self, hdfs_source_path, prefix_name, extension):
        command = f'source ~/.bash_profile; python3 /home/hadoop/bin/rename.py {hdfs_source_path} {prefix_name} {extension}'
        logger.info("exec_command: %s", command)
        stdin, stdout, stderr = self.ssh.exec_command(command)

        stdout.channel.set_combine_stderr(True)
        output = stdout.read().decode()

        logger.info("rename.py result: %s", output)


    def close(self):
        if self.ssh:
            self.ssh.close()
            self.ssh = None


if __name__ == '__main__':
    try:
        hdfs_source_path = 'hdfs://dataGWCluster/tmp/test'
        hdfs_dest_path = ' hdfs://dataGWCluster/tmp/test2'
        interface_id = 'C-TEST-DISTCP-0001'
        distcp_config = DistcpConfig(hdfs_source_path, hdfs_dest_path, interface_id, 0, 0)
        with DistcpExecutor() as executor:
            result = executor.exec(distcp_config)
            logger.info("Execution result: %s", result)
    except Exception as e:
        logger.exception("An error occurred")
