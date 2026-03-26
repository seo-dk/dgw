import logging
import os
import subprocess
from datetime import datetime
from pathlib import Path

from airflow.models import Variable

from commons_test.MetaInfoHook import InterfaceInfo
from commons_test.InfodatManager import InfoDatManager
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))


class PcellJarExecutor:
    def __init__(self, interface_info: InterfaceInfo, info_dat_path, partitions, kafka_send, collect_history):

        self.info_dat_path = info_dat_path
        self.partitions = partitions
        self.local_path = Path(interface_info.ftp_proto_info.ramdisk_dir, "in")
        self.out_path = Path(interface_info.ftp_proto_info.ramdisk_dir, "out")

        self.kafka_send = kafka_send
        self.collect_history = collect_history
        self.jar_path = "/local/util/PcellProcessor-1.0.jar"
        self.command = ['java', '-jar', self.jar_path, self.local_path, self.out_path]
        self.info = InfoDatManager()

    def start(self, file_info):
        work_dir = "/mnt/ramdisk/pcell"
        logger.info("work_dir : %s, local path: %s, local file count: %s", work_dir, self.local_path, len(os.listdir(self.local_path)))
        start_time = datetime.now()
        logger.info("jar process started at %s, command: %s", start_time.isoformat(), self.command)
        proc = subprocess.Popen(self.command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=work_dir)
        stdout, stderr = proc.communicate()
        output = stdout.decode('utf-8')
        errors = stderr.decode('utf-8')
        if stderr:
            self.kafka_send.sendErrorKafka(self.collect_history, 8, True, "external jar failure" + str(stderr)[:1024])
            raise Exception(f"Failed to call external jar file. {str(errors)}")

        end_time = datetime.now()
        logger.info("Finished jar process at %s, Time taken : %s", end_time.isoformat(), end_time - start_time)

        out_files = os.listdir(self.out_path)

        for file in out_files:
            out_file_path = Path(self.out_path, file)
            hdfs_path = self.partitions.get("hdfsPath")
            partitions = self.partitions.get("partitions")
            ulid = self.collect_history.start_collect_h(file_info.collect_source_seq, out_file_path, hdfs_path, file, file_info.file_proto_info.target_time, partitions)
            self.info.write_info_dat(out_file_path, hdfs_path, file, self.info_dat_path, ulid)
