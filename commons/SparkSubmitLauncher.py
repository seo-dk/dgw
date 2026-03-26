from dataclasses import dataclass, field, asdict
from typing import Union
import re
import paramiko
import logging
from pathlib import Path
from paramiko import SFTPClient
import json
from datetime import datetime
import base64
from typing import Union

from airflow.models import Variable
from commons.SchemaInfoManager import SchemaInfoManager
import commons.HdfsUtil as HdfsUtil

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))

@dataclass
class ParquetConversionConfig:
    delimiter: str = None
    source_format:str = None
    target_format: str = None
    target_compression: str = None
    source_compression: str = None
    encoding: str = None
    header_skip: bool = False
    merge_to_one_file: bool = False
    merge_file_nm: str = None

class SparksubmitLauncher:

    def __init__(self, hadoop_source_path, hadoop_output_path, memory_usage, tb_name, spark_server_info, pyspark_filenm):
        self.source_path = hadoop_source_path
        self.output_path = hadoop_output_path
        self.memory_usage = memory_usage
        self.tb_name = tb_name
        self.pyspark_filenm = pyspark_filenm
        self.ssh = None
        self.spark_server_info = spark_server_info

    def _init_ssh_client(self):
        try:
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh.connect(**{
                'hostname': self.spark_server_info.ip,
                'port': self.spark_server_info.port,
                'username': self.spark_server_info.username,
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

    def __enter__(self):
        if self.ssh is None:
            self._init_ssh_client()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def start(self, spark_config: Union[ParquetConversionConfig]):
        if self._is_source_file_exist(self.source_path):
            start_time = datetime.now()
            spark_schema_path, spark_argument = self._set_argument(spark_config)

            logger.info(f"exec file path : {self.spark_server_info.exec_file_path}")
            logger.info(f"encrtypt output_path : {self.output_path}")
            logger.info(f"spark_config : {spark_config}")

            stdout, stderr = self._start_spark_submit(spark_schema_path, spark_argument)

            exit_status, url_match, error_line = self._get_log_from_stdout(stdout, stderr)
            if exit_status == 0:
                logger.info(f"finished encryption, total process time : {datetime.now() - start_time}")
                return self.output_path
            else:
                logger.error(f"error : {error_line}")
                if url_match:
                    logger.info(f"log url : {url_match.group(0)}")
                raise Exception(f"Error occur during encryption, application_id : {self.app_id}, log url: {url_match.group((0))}")
        else:
            HdfsUtil.make_directory(self.output_path)
            logger.info(f"created empty dir, empty_dir path : {self.output_path}")
            return self.output_path


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

    def _set_argument(self, spark_config: Union[ParquetConversionConfig]):
        sftp = self.ssh.open_sftp()

        hdfs_dir = self._get_hdfs_dir()

        remote_schema_dir = Path(self.spark_server_info.exec_file_path).joinpath("schema")

        self._ensure_dir(sftp, remote_schema_dir)

        schema_file_name = self._get_arguments_file_name()

        spark_schema_path = Path(remote_schema_dir).joinpath(schema_file_name)

        if spark_config.source_format is None:
            logger.info("format is None")
        elif spark_config.source_format not in ['orc', 'parquet'] and spark_config.target_format in ['parquet', 'orc']:
            ## need to distcp to target server at here
            logger.info(f"hdfs_dir : {hdfs_dir}")
            schema_info = SchemaInfoManager(hdfs_dir).get_schema_dict()

            if not self._file_exist(sftp, spark_schema_path):
                logger.info(f"making schema file at remote server, file_nm : {spark_schema_path}")
                schema_str = ''
                if schema_info:
                    schema_str = json.dumps(schema_info)
                    with sftp.open(str(spark_schema_path), 'w') as file:
                        file.write(schema_str)
                else:
                    logger.info(f"There is no schema file with {hdfs_dir}")
        else:
            spark_schema_path = None
        logger.info(f"arguments file made : {spark_config}")
        sftp.close()

        encoded_spark_config = self._encode_to_base64(asdict(spark_config))

        return spark_schema_path, encoded_spark_config

    def _ensure_dir(self, sftp: SFTPClient, remote_dir):
        try:
            sftp.stat(str(remote_dir))
            return True
        except FileNotFoundError:
            logger.info(f"making directory : {remote_dir}")
            sftp.mkdir(remote_dir)

    def _file_exist(self, sftp: SFTPClient, remote_file):
        try:
            sftp.stat(str(remote_file))
            return True
        except FileNotFoundError:
            return False

    def _is_source_file_exist(self, input_file_pattern):
        file_exist = False
        try:
            need_encrypt_files = HdfsUtil.get_file_list(input_file_pattern)
            if need_encrypt_files:
                file_exist = True
        except Exception as e:
            logger.warning(f"Error : {e}")
            logger.info(f"{input_file_pattern} is empty..")
        finally:
            return file_exist

    def _get_hdfs_dir(self):
        match = re.search(r'/db=[^/]+/tb=[^/]+', self.source_path)
        return match.group(0)

    def _get_arguments_file_name(self):
        match = re.search(r'db=([^/]+)/tb=([^/]+)', self.source_path)
        if match:
            db, tb = match.group(1), match.group(2)
            return f"{db}.{tb}.json"
        else:
            logger.info("There is no db, tb value in hdfs source path...")
            logger.info(f"hadoop source path : {self.source_path}")

    def _encode_to_base64(self, context):
        json_string = json.dumps(context)
        encoded_result = base64.b64encode(json_string.encode()).decode()
        return encoded_result

    def _start_spark_submit(self, spark_schema_path, spark_argument):
        exec_command = (f'cd "{self.spark_server_info.exec_file_path}" && '
                        f'export SCHEMA_LOCAL_PATH="{spark_schema_path}" && '
                        f'export SCHEMA_HADOOP_PATH="{self.spark_server_info.hadoop_schema_path}" && '
                        f'sh run_v2.sh "{self.source_path}" "{self.output_path}" "{self.tb_name}" '
                        f'"{self.memory_usage}" "{self.spark_server_info.queue}" "{self.spark_server_info.exec_file_path}" '
                        f'"{self.pyspark_filenm}" "{spark_argument}"')

        logger.info("exec_command: %s", exec_command)

        logger.debug("start_spark_process")
        _, stdout, stderr = self.ssh.exec_command(exec_command)
        return stdout, stderr
