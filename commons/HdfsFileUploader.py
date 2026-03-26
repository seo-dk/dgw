import concurrent.futures
import glob
import logging
import os
from datetime import datetime

from airflow.exceptions import AirflowException
from airflow.models import Variable
from pyarrow.fs import HadoopFileSystem, LocalFileSystem, FileType

from commons.CompressionManager import CompressionManager
from commons.InfodatManager import InfoDatManager

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))

hadoop_home = '/local/HADOOP'

pattern = [hadoop_home + '/share/hadoop/' + d + '/**/*.jar' for d in ['hdfs', 'common']]
hdfs_cp = ':'.join(file for p in pattern for file in glob.glob(p, recursive=True))

os.environ['CLASSPATH'] = ':'.join([hadoop_home + '/etc/hadoop:', hdfs_cp])
os.environ['LD_LIBRARY_PATH'] = os.getenv('LD_LIBRARY_PATH', '') + ':' + hadoop_home + '/lib/native:'


class HdfsFileUploader:
    def __init__(self, kafka_send, collect_history, delimiter, info_dat_path, encoding, stdout=False, retry=False, cleanup_hdfs_dir=False, port=8020, user='hadoop'):
        self.kafka = kafka_send
        self.collect_history = collect_history
        self.delimiter = delimiter
        self.info_dat_path = info_dat_path
        self.encoding = encoding
        self.stdout = stdout
        self.retry = retry
        self._hdfs = self._initialize_hdfs_connection(port, user)
        self.local_fs = LocalFileSystem()
        self.producer_conf = {'bootstrap.servers': 'tvdn-012-061:9093'}
        self.infodat_manager = InfoDatManager()
        self.compression_manager = CompressionManager()

        if cleanup_hdfs_dir:
            self._cleanup_directories_from_info()

    def _initialize_hdfs_connection(self, port, user):
        try:
            return HadoopFileSystem("default", port=port, user=user)
        except Exception as e:
            self.kafka.sendErrorKafka(self.collect_history, 4, True, "Error connecting HDFS")
            raise AirflowException("HDFS connection failed") from e

    def _cleanup_directories_from_info(self):
        try:
            if os.path.exists(self.info_dat_path):
                with open(self.info_dat_path, 'r') as f:
                    lines = f.readlines()

                for line in lines:
                    dest_full = line.strip().split(",")[1]
                    dest = os.path.dirname(dest_full)
                    logger.info("Checking and cleaning HDFS directory: %s", dest)
                    self._clear_hdfs_directory(dest)
        except FileNotFoundError as e:
            logger.exception("Info file does not exist: %s", self.info_dat_path)

    def _clear_hdfs_directory(self, dest):
        try:
            self._hdfs.delete_dir_contents(dest)
            logger.info("Deleted contents of HDFS directory: %s", dest)
        except Exception:
            logger.info("HDFS directory does not exist or is already empty: %s", dest)

    def _copy(self, local_path, dest, ulid, stdout=False, retry=False, encoding='UTF-8'):
        chd = self.collect_history.chdDict[ulid]
        file_name = os.path.basename(dest)
        buffer_size = self._calculate_buffer_size(local_path)

        try:
            self._prepare_hdfs_destination(dest)

            if self.compression_manager.check_gz_extension(local_path) or local_path.endswith(".zip"):
                self._copy_file_raw(local_path, dest, buffer_size)
            elif local_path.endswith(".parquet") or encoding == 'UTF-8':
                self._copy_file_raw(local_path, dest, buffer_size)
            else:
                self._copy_file_with_encoding(local_path, dest, encoding, buffer_size)

            self._finalize_copy_success(chd, file_name, dest, ulid, stdout, retry)
            return "Success"

        except Exception as e:
            self._handle_copy_failure(chd, file_name, local_path, dest, e)

    def start(self):
        if not os.path.isfile(self.info_dat_path):
            logger.warning("File not exists: %s", self.info_dat_path)
            return

        with open(self.info_dat_path, 'r') as f:
            tasks = [line.strip().split(",") for line in f]

        start_time = datetime.now()
        results = self._process_copy_tasks(tasks)

        self._log_summary(results, start_time)

    def _prepare_hdfs_destination(self, dest):
        dest_path = os.path.dirname(dest.strip())
        if self._hdfs.get_file_info(dest_path).type != FileType.Directory:
            self._hdfs.create_dir(dest_path, recursive=True)

    def _copy_file_raw(self, local_path, dest, buffer_size):
        logger.info("copy local to hdfs: %s -> %s", local_path, dest)
        with self.local_fs.open_input_stream(local_path) as local_file, \
             self._hdfs.open_output_stream(dest) as hdfs_file:
            while True:
                chunk = local_file.read(buffer_size)
                if not chunk:
                    break
                hdfs_file.write(chunk)

    def _copy_file_with_encoding(self, local_path, dest, encoding, buffer_size):
        logger.info("copy local(encoding: %s) to hdfs: %s -> %s", encoding, local_path, dest)
        with open(local_path, 'r', encoding=encoding, errors='replace') as local_file, \
             self._hdfs.open_output_stream(dest) as hdfs_file:
            while True:
                chunk = local_file.read(buffer_size)
                if not chunk:
                    break
                hdfs_file.write(chunk.encode('utf-8'))

    def _calculate_buffer_size(self, local_path):
        file_size = os.path.getsize(local_path)
        if file_size >= 10 * 1024**3:
            return 1024**3
        elif file_size >= 1024**3:
            return 512 * 1024**2
        elif file_size >= 64 * 1024**2:
            return 128 * 1024**2
        return 64 * 1024**2

    def _process_copy_tasks(self, tasks):
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(self._copy, local_path, dest, ulid, self.stdout, self.retry, self.encoding)
                       for local_path, dest, ulid in tasks]
            return [future.result() for future in concurrent.futures.as_completed(futures)]

    def _finalize_copy_success(self, chd, file_name, dest, ulid, stdout, retry):
        file_info = self._hdfs.get_file_info(dest)
        self.collect_history.set_partition_info(
            ulid, file_name, file_info.size, datetime.now().isoformat(), dest
        )
        self.collect_history.set_status_code(chd, 4)

        if os.path.splitext(file_name)[1] == ".parquet":
            self.collect_history.set_noti_version("2", ulid)
            self.collect_history.set_protoco_cd("DISTCP", ulid)
        else:
            self.collect_history.set_noti_version("1", ulid)

        chd.retry = False if retry else chd.retry
        self.kafka.send_to_kafka(chd)
        if stdout:
            logger.info("Upload Success: %s -> %s", file_name, dest)

    def _handle_copy_failure(self, chd, file_name, local_path, dest, exception):
        chd.ended_at = datetime.now().isoformat()
        chd.err_msg = f"Error file: {file_name} {exception}"
        self.collect_history.set_status_code(chd, 3)
        self.kafka.send_to_kafka(chd)

        raise Exception("Copy failed: %s -> %s", local_path, dest)

    def _log_summary(self, results, start_time):
        success_count = results.count("Success")
        total_count = len(results)
        end_time = datetime.now()
        time_taken = end_time - start_time

        logger.info("%d/%d files uploaded successfully.", success_count, total_count)
        logger.info("HDFS upload Total Time taken: %s", time_taken)

        if success_count < total_count:
            logger.error("Some files failed to upload. Success: %d, Total: %d", success_count, total_count)
