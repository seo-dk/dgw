import os.path

from airflow.exceptions import AirflowException
from datetime import datetime
import pendulum
import logging
from airflow.models import Variable
import socket
import paramiko
import json

from commons_test.MetaInfoHook import MetaInfoHook
from commons_test.CollectHistory import CollectHistory
from commons_test.KafkaSend import KafkaSend

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))


class DistcpTransfer:
    SUCCESS = 4
    DATA_INQUALITY = 5

    CMD_TEMPLATE = 'source ~/.bash_profile; python3 /home/hadoop/bin/distcp3.py {0} {1} {2}'

    SSH_PARAMS = {
        'hostname': '90.90.47.63',
        'port': 22,
        'username': 'hadoop',
        'password': 'gkdid2023^_',
        'look_for_keys': False,
        'allow_agent': False
    }

    def __init__(self, context, dag_id, task_id):
        self._init_context_vars(context)

        currentHostName = socket.gethostname()
        self.collectHistory = CollectHistory(self.interface_id, dag_id, task_id, currentHostName, "1",
                                             self.execution_date)

        self.kafka_send = KafkaSend()

        metaInfoHook = MetaInfoHook(self.kafka_send, self.collectHistory)
        self.metaInfo = metaInfoHook.GetMetaInfo(self.interface_id)
        self.folderInfo = metaInfoHook.GetSourceFolderInfo(self.interface_id)
        self.statusCodeInfo = metaInfoHook.GetStatusCode()
        metaInfoHook.dbSessionClose()

        self.kafka_send.set_status_code(self.statusCodeInfo)

        self.collectHistory.setServerInfo(self.metaInfo)
        self.collectHistory.server_info.period = self._get_period(self.metaInfo.DISTCP_SERVER_INFO.PERIOD,
                                                                  self.src_path)
        self.collectHistory.set_statuscode_info(self.statusCodeInfo)

        self._prepare_paths()

    def _init_context_vars(self, context):
        dag_run = context['dag_run']
        self.interface_id = dag_run.conf.get('interface_id')
        self.src_path = dag_run.conf.get('source_path')
        self.dest_path = dag_run.conf.get('destination_path')
        self.partitions = dag_run.conf.get('partitions')
        self.execution_date = context['execution_date']

        logger.info("dag_run.conf: %s", dag_run.conf)

    def _prepare_paths(self):
        self.partitions_list = []
        self.partitions_str = ""
        self.target_dt = ""
        for key, value in self.partitions.items():
            if key and value:
                self.partitions_list.append({key: value})
                if key in ['dt', 'hh', 'ym']:
                    self.target_dt += str(value)
                self.partitions_str += f"/{key}={value}"
        self.src_path += self.partitions_str

        self.hdfs_source_path = f'{self.metaInfo.DISTCP_SERVER_INFO.SOURCE_DIR}{self.src_path}'
        logger.info('hdfs_source_path: %s', self.hdfs_source_path)

    def _get_hdfs_dest_path(self, file_info):
        if self.metaInfo.DISTCP_SERVER_INFO.RENAME_TABLE:
            logger.info("RENAME_TABLE")
            _hdfs_dest_path = '/idcube_out' + file_info.HDFS_DIR + self.partitions_str
        else:
            _hdfs_dest_path = self.dest_path + self.partitions_str

        logger.info("target_dt : %s, src_path : %s, hdfs_dest_path : %s", self.target_dt, self.src_path,
                    _hdfs_dest_path)

        return _hdfs_dest_path

    def _init_ssh_client(self):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        ssh.connect(**DistcpTransfer.SSH_PARAMS)
        return ssh

    def _handle_msg_code(self, json_output, chd):
        self.collectHistory.setStatusCode(chd, json_output['code'])
        self.kafka_send.send_to_kafka(chd)
        message = json_output.get('message', '')
        msg_code = json_output['code']

        logger.info('message: %s', message)

        if msg_code != DistcpTransfer.SUCCESS:
            self.kafka_send.sendErrorKafka(self.collectHistory, msg_code, False, message)
            if msg_code != DistcpTransfer.DATA_INQUALITY:
                raise AirflowException("Failed to distcp")

    def _initiate_file_transfer(self, file_info, ssh):

        chd_ulid = self.collectHistory.startCollectH(
            collect_source_seq=file_info.COLLECT_SOURCE_SEQ,
            local_path=self.hdfs_source_path,
            dest_path=self.hdfs_dest_path,
            filenm="",
            target_dt=self.target_dt,
            partitions=self.partitions_list
        )

        chd = self.collectHistory.chdDict[chd_ulid]
        self.collectHistory.setStatusCode(chd, 0)
        self.kafka_send.send_to_kafka(chd)

        return chd_ulid, chd

    def _execute_command(self, ssh):
        str_command = DistcpTransfer.CMD_TEMPLATE.format(self.hdfs_source_path, "hdfs://dataGWCluster" + self.hdfs_dest_path, self.interface_id)
        logger.info("exec_command: %s", str_command)
        _, stdout, _ = ssh.exec_command(str_command)

        stdout_lines = stdout.readlines()
        json_output = json.loads("".join(stdout_lines))
        logger.info("json_output: %s", json_output)
        stdout.channel.set_combine_stderr(True)
        return json_output

    def _handle_command(self, chd, chd_ulid, json_output):
        current_time = datetime.now().isoformat()
        chd.ended_at = current_time

        self.collectHistory.setPartitionInfo(chd_ulid, '', json_output.get('dest_size', -1), current_time,
                                             self.hdfs_source_path)
        self._handle_msg_code(json_output, chd)

    def _distcp(self):
        ssh = None
        try:
            ssh = self._init_ssh_client()
            for file_info in self.folderInfo:
                if file_info.DISTCP_FILE_INFO.FILE_NM.upper() in self.src_path.upper():
                    self.hdfs_dest_path = self._get_hdfs_dest_path(file_info)
                    chd_ulid, chd = self._initiate_file_transfer(file_info, ssh)
                    json_output = self._execute_command(ssh)
                    self._handle_command(chd, chd_ulid, json_output)
        finally:
            if ssh is not None:
                ssh.close()


    def execute(self):
        try:
            self._distcp()
        except Exception as e:
            self.kafka_send.sendErrorKafka(self.collectHistory, 7, True, str(e)[:1024])
            logger.exception(f"Transfer_Distcp error: {e}")
            raise AirflowException("Error at distcp : " + str(e)[:1024])
        finally:
            self.kafka_send.closeKafka()

    def _get_period(self, period, src_path):
        period_mapping = {
            '1h': "0 * * * *",
            '1d': "0 0 * * *",
            '1w': "0 0 * * 4",
            '1m': "0 0 1-7 * 5"
        }
        path_period_mapping = {
            'period=1h': "0 * * * *",
            'period=1d': "0 0 * * *",
            'period=1w': "0 0 * * 4",
            'period=1m': "0 0 1-7 * 5"
        }
        if period:
            return period_mapping[period]
        else:
            return next((value for key, value in path_period_mapping.items() if key in src_path), "")
