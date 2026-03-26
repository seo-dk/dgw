import concurrent.futures
import logging
import os
import re
import signal
import sys
import shutil
import pathlib
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from airflow.exceptions import AirflowException
from airflow.models import Variable

from commons.ConvertToParquet2_v2 import convert
from commons.MetaInfoHook import SourceInfo, InterfaceInfo, SparkServerInfo
import commons.HdfsUtil as HdfsUtil
from commons.SparkSubmitLauncher import SparksubmitLauncher, ParquetConversionConfig

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))


class DistcpParquetHandler:
    def __init__(self, source_info: SourceInfo, interface_info: InterfaceInfo, hdfs_src_path):
        self.source_info = source_info
        self.interface_info = interface_info
        self.hdfs_src_path = hdfs_src_path
        self._set_source_info_hdfs_dir()

        self.process = None
        self.app_id = None
        signal.signal(signal.SIGINT, self._on_interrupt)

    def _on_interrupt(self, signum, frame):
        if self.process:
            self.process.terminate()
        if self.app_id:
            subprocess.run(["yarn", "application", "-kill", self.app_id])
        sys.exit(1)

    def _set_source_info_hdfs_dir(self):
        if not "/tb=" in self.source_info.hdfs_dir:
            pattern = re.compile(r'/tb=([^/]+)')
            match = pattern.search(self.hdfs_src_path)

            if match:
                tb_name = match.group(1).lower()
                self.source_info.hdfs_dir = self.source_info.hdfs_dir + f"/tb={tb_name}"
                logger.info("self.source_info.hdfs_dir: %s", self.source_info.hdfs_dir)

    def _convert_in_local(self):
        hdfs_src_path = "hdfs://dataGWCluster" + self.hdfs_src_path
        local_download_path = '/data' + self.hdfs_src_path.replace("/staging", "")
        local_parquet_path = local_download_path.replace("/idcube_out", "/idcube_out/converted_parquet")

        self._download(hdfs_src_path, local_download_path)

        # convert
        if os.path.exists(local_parquet_path):
            shutil.rmtree(local_parquet_path)
            logger.info("local_parquet_path removed for retry: %s", local_parquet_path)

        os.makedirs(local_parquet_path, exist_ok=True)

        local_download_files_pattern = str(local_download_path) + "/*"
        convert(local_download_files_pattern, self.source_info, self.interface_info, None, None)

        # for retry
        HdfsUtil.remove_files(self.hdfs_src_path.replace("/staging", ""))

        hdfs_path = os.path.join("hdfs://dataGWCluster", self.hdfs_src_path.replace("/staging", "")).rsplit('/', 1)[0]
        HdfsUtil.upload_files(local_parquet_path, hdfs_path)

    def _convert_in_spark(self):
        if not self.source_info.distcp_proto_info.spark_server_info.is_external:
            ## hdfs_src_path : /staging/idcube_out/db=o_tpani/tb=user_hdvoice_info/dt=20250515
            hdfs_src_path = "hdfs://dataGWCluster" + self.hdfs_src_path
            spark_server_info = SparkServerInfo()
        else:
            ## hdfs_src_path : hdfs://90.90.43.21:8020/tap_d/staging/db=o_datalake/tb=pin_cluster/dt=20250526/
            hdfs_src_path = self.hdfs_src_path
            spark_server_info = self.source_info.distcp_proto_info.spark_server_info

        hdfs_output_path = hdfs_src_path.replace("/staging", "")

        tb_match = re.search(r'tb=([^/]+)', self.hdfs_src_path)
        tb_name = tb_match.group(1) if tb_match else None

        spark_config = ParquetConversionConfig(self.interface_info.distcp_proto_info.delimiter, self.source_info.distcp_proto_info.format,
                                               "parquet", self.source_info.distcp_proto_info.target_compression,
                                               self.source_info.distcp_proto_info.source_compression, self.interface_info.distcp_proto_info.encode)

        with SparksubmitLauncher(hdfs_src_path, hdfs_output_path, self.source_info.distcp_proto_info.enc_memory_usage,
                                 tb_name, spark_server_info, "parquet_conversion.py") as spark_launcher:
            _ = spark_launcher.start(spark_config)

    def start_convert(self):
        if self.source_info.distcp_proto_info.convert_in_spark:
            self._convert_in_spark()
        else:
            self._convert_in_local()

    def _download(self, src_path, dest_path):
        logger.info('downloading files...')

        success_file_path = pathlib.Path(dest_path) / '_SUCCESS'
        if success_file_path.exists():
            logger.info("_SUCCESS file already exists at: %s. Skipping download.", success_file_path)
            return

        hdfs_decomp_path = src_path.replace("/staging", "/staging/decomp")

        if self.source_info.distcp_proto_info.format == 'lz4':
            if HdfsUtil.check_path(hdfs_decomp_path):
                HdfsUtil.remove_diretory(hdfs_decomp_path)
                logger.info("hdfs decomp path removed for retry: %s", hdfs_decomp_path)
            self._hdfs_decomp_lz4_to_gz(src_path, hdfs_decomp_path)

            if os.path.exists(dest_path):
                shutil.rmtree(dest_path)
                logger.info("dest_path removed for retry: %s", dest_path)

            os.makedirs(dest_path, exist_ok=True)

            hdfs_decomp_files = os.path.join(hdfs_decomp_path, "*.gz")
            HdfsUtil.get_files(hdfs_decomp_files, dest_path)

            self._validate_file_count(hdfs_decomp_path, dest_path)
        else:
            dest_path = dest_path.rsplit('/', 1)[0]

            if os.path.exists(dest_path):
                shutil.rmtree(dest_path)
                logger.info("dest_path removed for retry: %s", dest_path)

            os.makedirs(dest_path, exist_ok=True)
            HdfsUtil.get_files(src_path, dest_path)

    def _hdfs_decomp_lz4_to_gz(self, hdfs_path, dest_path):
        start_time = datetime.now()
        hadoop_command = None
        codec = "org.apache.hadoop.io.compress.GzipCodec"
        logger.info("_hdfs_decomp_lz4_to_gz hdfs_path: %s, dest_path: %s", hdfs_path, dest_path)

        match = re.search(r"db=(\S+)/tb=(\S+)/period=\S+/dt=(\d+)(?:/hh=(\d+))?", hdfs_path)
        if not match:
            raise ValueError("Input path does not contain the expected dt/hh structure")

        db, tb, date, hour = match.groups()

        if hour:
            job_name = f"decomp: {db}.{tb}_{date}_{hour}"
            app_tags = f"decomp,{db}.{tb},{hour}"
        else:
            job_name = f"decomp: {db}.{tb}_{date}"
            app_tags = f"decomp,{db}.{tb}"

        try:
            hadoop_command = [
                "yarn", "jar", "/local/HADOOP/share/hadoop/tools/lib/hadoop-streaming-3.3.4.jar",
                "-D", "mapreduce.output.fileoutputformat.compress=true",
                "-D", f"mapreduce.output.fileoutputformat.compress.codec={codec}",
                "-D", "mapreduce.job.reduces=0",
                "-D", "mapreduce.map.memory.mb=4096",
                "-D", "mapreduce.map.cpu.vcores=2",
                "-D", "mapreduce.input.fileinputformat.split.minsize=134217728",
                "-D", "mapreduce.input.fileinputformat.split.maxsize=268435456",
                "-D", f"mapreduce.job.name={job_name}",
                "-D", f"mapreduce.job.tags={app_tags}",
                "-D", f"yarn.application.tags={app_tags}",
                "-input", hdfs_path,
                "-output", dest_path,
                "-mapper", "/bin/cat"
            ]

            self.process = subprocess.Popen(hadoop_command, preexec_fn=os.setsid, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            result = subprocess.run(["yarn", "application", "-list"], stdout=subprocess.PIPE, text=True)

            for line in result.stdout.splitlines():
                if job_name in line:
                    self.app_id = line.split()[0]
                    break

            self.process.wait()

        except subprocess.CalledProcessError as e:
            raise AirflowException(f"Error occurred: {e.stderr}")

        finally:
            if self.app_id:
                subprocess.run(["yarn", "application", "-kill", self.app_id])
            if self.process and self.process.poll() is None:
                self.process.terminate()
            logger.info("hdfs_decomp_lz4_to_gz cmd: %s, elapsed time: %s", hadoop_command, datetime.now() - start_time)

    def _get_parquet_conversion_paths(self, path):
        parquet_conversion_paths = []

        for entry in os.listdir(path):
            entry_path = os.path.join(path, entry)
            if os.path.isfile(entry_path):
                if "_SUCCESS" not in entry_path:
                    parquet_conversion_paths.append(entry_path)

        return parquet_conversion_paths

    def _validate_file_count(self, src_path, dest_path):
        src_file_count = 0

        cmd = f"hdfs dfs -ls {src_path} | grep '.gz' | wc -l"

        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            src_file_count = int(result.stdout)
        else:
            raise AirflowException(f"Error occurred: {result.stderr}")

        local_file_count = 0

        for file_name in os.listdir(dest_path):
            file_path = os.path.join(dest_path, file_name)
            if os.path.isfile(file_path):
                local_file_count = local_file_count + 1

        if src_file_count == local_file_count:
            success_file_path = pathlib.Path(dest_path) / '_SUCCESS'
            success_file_path.touch()
            logger.info("count of src files and local files is correct, count: %s", local_file_count)
            logger.info("_SUCCESS file created at: %s", success_file_path)
        else:
            raise AirflowException("count of lz4 files and downloaded files is different, src_file_count: %s, local_file_count: %s", src_file_count, local_file_count)
