import glob
import os
import fastavro
import csv
import ftplib
from datetime import datetime, timedelta
from pyarrow import fs
from pyarrow.fs import HadoopFileSystem
from concurrent.futures import ThreadPoolExecutor, wait

import logging_config
import logging

logging_config.setup_logging(__file__)
logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])


class DnaBadEventHandler:
    def __init__(self, hdfs_base_path, work_path, hadoop_home='/local/HADOOP', max_workers=20):
        self.hadoop_home = hadoop_home
        self.hdfs_base_path = hdfs_base_path
        self.work_path = work_path
        self.max_workers = max_workers
        self.hdfs = self._setup_hadoop_filesystem()

    def _setup_hadoop_filesystem(self):
        pattern = [self.hadoop_home + '/share/hadoop/' + d + '/**/*.jar' for d in ['hdfs', 'common']]
        hdfs_cp = ':'.join(file for p in pattern for file in glob.glob(p, recursive=True))

        os.environ['CLASSPATH'] = ':'.join([self.hadoop_home + '/etc/hadoop:', hdfs_cp])
        os.environ['LD_LIBRARY_PATH'] = os.getenv('LD_LIBRARY_PATH', '') + ':' + self.hadoop_home + '/lib/native:'

        return HadoopFileSystem('default', port=8020, user='hadoop')

    def _avro_to_csv(self, thread_number, avro_file_path, date_suffix):
        try:
            csv_file_path = self._prepare_csv_file_path(date_suffix, thread_number)
            self._process_avro_file(self._download_avro_file(avro_file_path), csv_file_path)
        except Exception as e:
            logging.exception(f'Failed to process avro file and upload: avro_file_path: {avro_file_path}, date_suffix: {date_suffix}')

    def _prepare_csv_file_path(self, date_suffix, thread_number):
        csv_file_name = f"dna_bad_event_loc_ib_{date_suffix}_{thread_number}.dat"
        return os.path.join(self.work_path, csv_file_name)

    def _download_avro_file(self, avro_file_path):
        base_name = os.path.basename(avro_file_path)
        local_avro_file_path = os.path.join(self.work_path, base_name)

        with self.hdfs.open_input_stream(avro_file_path) as hdfs_file, open(local_avro_file_path, 'wb') as local_file:
            start_time = datetime.now()
            logger.info('download started. %s -> %s', avro_file_path, local_avro_file_path)
            while True:
                chunk = hdfs_file.read(8192)
                if not chunk:
                    break
                local_file.write(chunk)

            logger.info('download completed. %s -> %s. duration: %s', avro_file_path, local_avro_file_path, datetime.now() - start_time)

        return local_avro_file_path

    def _write_chunk(self, csv_file, chunk, csv_file_path):
        writer = csv.DictWriter(csv_file, fieldnames=chunk[0].keys(), delimiter='\036', escapechar='\\', quoting=csv.QUOTE_NONE)
        for record in chunk:
            writer.writerow(record)

    def _process_avro_file(self, local_avro_file_path, csv_file_path, chunk_size=10000):
        with open(csv_file_path, 'a', newline='') as csv_file, open(local_avro_file_path, 'rb') as avro_file:
            reader = fastavro.reader(avro_file)
            current_chunk = []

            for record in reader:
                current_chunk.append(record)
                if len(current_chunk) == chunk_size:
                    self._write_chunk(csv_file, current_chunk, csv_file_path)
                    current_chunk.clear()

            if current_chunk:
                self._write_chunk(csv_file, current_chunk, csv_file_path)

        logger.info('completed to convert avro to csv. %s -> %s', local_avro_file_path, csv_file_path)

    def _upload_to_ftp(self, file_path, ftp_server, base_ftp_path, username, password, date_suffix):
        with ftplib.FTP(ftp_server) as ftp:
            ftp.login(username, password)
            ftp_path = base_ftp_path

            try:
                ftp.cwd(ftp_path)
            except ftplib.error_perm:
                ftp.mkd(ftp_path)
                ftp.cwd(ftp_path)

            with open(file_path, 'rb') as file:
                ftp.storbinary(f'STOR {os.path.basename(file_path)}', file)

            logger.info(f'upload completed: {file_path}, date_suffix: {date_suffix}')

    def _merge_files(self, output_file_name, file_pattern):
        with open(output_file_name, 'wb') as merged_file:
            for file_path in glob.glob(file_pattern):
                with open(file_path, 'rb') as individual_file:
                    merged_file.write(individual_file.read())
                os.remove(file_path)

            logger.info('merge completed, output_file_name: %s', output_file_name)

    def _cleanup_work_path(self):
        for file in os.listdir(self.work_path):
            file_path = os.path.join(self.work_path, file)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    logger.info(f'Deleted file: {file_path}')
            except Exception as e:
                logger.exception(f'Failed to delete file: {file_path}')

    def process(self):
        date_suffix = (datetime.now() - timedelta(days=4)).strftime("%Y%m%d")
        hdfs_avro_path = os.path.join(self.hdfs_base_path, f"dt={date_suffix}")

        try:
            file_info_list = self.hdfs.get_file_info(fs.FileSelector(hdfs_avro_path))
            avro_files = [file_info.path for file_info in file_info_list if file_info.path.endswith('.avro')]

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(self._avro_to_csv, idx, avro_file_path, date_suffix) for idx, avro_file_path in enumerate(avro_files)]
                wait(futures)

            merged_file_name = os.path.join(self.work_path, f"dna_bad_event_loc_ib_{date_suffix}.dat")
            file_pattern = os.path.join(self.work_path, f"dna_bad_event_loc_ib_{date_suffix}_*.dat")
            self._merge_files(merged_file_name, file_pattern)

            self._upload_to_ftp(merged_file_name, '90.90.40.86', '/data03/TANGOD/v2', 'aomuser', 'apdldhdpa123!', date_suffix)
            self._cleanup_work_path()
        except Exception as e:
            logger.exception(f'Failed to process dna_bad_event_lo_ib. hdfs_avro_path: {hdfs_avro_path}')


if __name__ == "__main__":
    handler = DnaBadEventHandler("/idcube_out/db=o_common_raw/tb=dna_bad_event_loc_ib/", "/mnt/ramdisk/tpani/")
    handler.process()

