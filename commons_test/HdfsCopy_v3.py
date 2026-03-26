import argparse
import concurrent.futures
import glob
import logging
import os
from datetime import datetime

import chardet
from airflow.exceptions import AirflowException
from airflow.models import Variable
from pyarrow.fs import HadoopFileSystem, LocalFileSystem, FileType
from billiard import Pool, cpu_count

from commons_test.CollectHistory import CollectHistory
from commons_test.CompressionManager import CompressionManager
from commons_test.InfodatManager import InfoDatManager

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))

hadoop_home = '/local/HADOOP'

pattern = [hadoop_home + '/share/hadoop/' + d + '/**/*.jar' for d in ['hdfs', 'common']]
hdfs_cp = ':'.join(file for p in pattern for file in glob.glob(p, recursive=True))

os.environ['CLASSPATH'] = ':'.join([hadoop_home + '/etc/hadoop:', hdfs_cp])
os.environ['LD_LIBRARY_PATH'] = os.getenv('LD_LIBRARY_PATH', '') + ':' + hadoop_home + '/lib/native:'

host = "default"
port=8020
user='hadoop'

def _copy_local_to_hdfs(local_path, dest, encoding, hdfs):
    logger.info("copy local(encoding: %s) to hdfs: %s -> %s", encoding, local_path, dest)

    with open(local_path, 'r', encoding=encoding, errors='replace') as local_file:
        # hdfs = HadoopFileSystem(host, port=port, user=user)
        with hdfs.open_output_stream(dest) as hdfs_file:
            while True:
                line = local_file.readline()
                if not line:
                    break
                hdfs_file.write(line.encode('utf-8'))

def _copy_raw_file_to_hdfs(local_path, dest, hdfs, buffer_size=16 * 1024 * 1024):
    with LocalFileSystem().open_input_stream(local_path) as local_file:
        # hdfs = HadoopFileSystem(host, port=port, user=user)
        with hdfs.open_output_stream(dest) as hdfs_file:
            while True:
                chunk = local_file.read(buffer_size)
                if not chunk:
                    break
                hdfs_file.write(chunk)

def copy(local_path, dest, ulid, stdout, retry, db_encoding_info, collectHistory):
    # local_path, dest, ulid, stdout, retry, db_encoding_info, collectHistory, kafka = args
    chd = collectHistory.chdDict[ulid]
    file_name = os.path.basename(dest)

    logger.info("local_path : %s", local_path)
    try:
        logger.info("hdfs path : %s", dest)
        hdfs = HadoopFileSystem(host, port=port, user=user)

        dest_path = os.path.dirname(dest.strip())

        file_info = hdfs.get_file_info(dest_path)
        if file_info.type != FileType.Directory:
            hdfs.create_dir(dest_path, recursive=True)

        if hdfs.get_file_info(dest_path).type == FileType.NotFound:
            logger.error("dest path not exists. %s", dest_path)

        start_time = datetime.now()
        logger.info("start opening to read data file %s...", local_path)

        if CompressionManager().check_gz_extension(local_path) or local_path.endswith(".zip"):
            logger.info("copy gz/zip to hdfs: %s -> %s", local_path, dest)
            _copy_raw_file_to_hdfs(local_path, dest, hdfs)
        else:
            logger.info("local_path : %s, Encoding info from db : %s", local_path, db_encoding_info)
            encoding = db_encoding_info
            if encoding is None:
                encoding = db_encoding_info
            if local_path.endswith(".parquet"):
                logger.info("copy parquet local to hdfs: %s -> %s", local_path, dest)
                _copy_raw_file_to_hdfs(local_path, dest, hdfs)
            else:
                _copy_local_to_hdfs(local_path, dest, encoding, hdfs)

        end_time = datetime.now()
        time_taken = end_time - start_time
        logger.info("HDFS upload success, File : %s , Time taken : %s ", file_name, time_taken)

        current_time = end_time.isoformat()
        file_size = hdfs.get_file_info(dest).size
        collectHistory.set_partition_info(ulid, file_name, file_size, current_time, local_path)
        collectHistory.set_status_code(chd, 4)

        if os.path.splitext(file_name)[1] == ".parquet":
            logger.info("changing version")
            collectHistory.set_noti_version("2", ulid)
            collectHistory.set_protoco_cd("DISTCP", ulid)
        else:
            collectHistory.set_noti_version("1", ulid)

        if retry:
            chd.retry = False
        # kafka.send_to_kafka(chd)

        if stdout:
            logger.info("Succ : %s ==> %s", local_path, dest)
        return "Success"
    except Exception as e:
        collectHistory.set_status_code(chd, 3)
        chd.ended_at = datetime.now().isoformat()
        chd.err_msg = f"Error file : {file_name} " + str(e)
        # kafka.send_to_kafka(chd)
        logger.error("HDFS copy Failed, local_path :  %s, dest : %s, file name : %s," % (local_path, dest, file_name))

def main(kafka_send, collect_history, info_dat_path, encoding=None, stdout=False, retry=False):
    if not os.path.isfile(info_dat_path):
        logger.warning("file not exitst. local_info_file_path: %s", info_dat_path)
        return

    with open(info_dat_path, 'r') as f:
        lines = f.readlines()

    logger.info("HDFS main")
    start_time = datetime.now()

    results = []
    with Pool(processes=5) as pool:
        tasks = [(local_path, dest, ulid, stdout, retry, encoding, collect_history)
                 for local_path, dest, ulid in [line.strip().split(",") for line in lines]]
        results = pool.starmap(copy, tasks, chunksize=1)
        pool.close()
        pool.join()


    end_time = datetime.now()
    time_taken = end_time - start_time
    logger.info("HDFS upload Total Time taken : %s ",  time_taken)

    # with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    #     futures = [executor.submit(copy, local_path, dest, ulid, stdout, retry, encoding)
    #                for local_path, dest, ulid in [line.strip().split(",") for line in lines]]
    #     for future in concurrent.futures.as_completed(futures):
    #         results.append(future.result())

    result_count = results.count("Success")
    total = len(results)
    try:
        response_code = result_count // total
        logger.info("%s / %s has been stored at hdfs", result_count, total)
        if response_code < 1:
            logger.error("Hdfs copy failed, Hdfs copy result: %s / %s", result_count, total)
    except Exception as e:
        logger.error(str(e))

def check_dir(info):
    try:
        # read info.dat file
        if os.path.exists(str(info)):
            with open(info, 'r') as f:
                lines = f.readlines()

                for line in lines:
                    list = line.strip().split(",")
                    dest_full = list[1]
                    dest_list = dest_full.split('/')
                    dest = '/'.join(dest_list[:-1])
                    logger.info("dest path : %s", dest)
                    # delete(dest)

    except FileNotFoundError as e:
        logger.exception("info file dose not exits. info: %s", info)

def delete(dest):
    try:
        hdfs = HadoopFileSystem(host, port=port, user=user)
        hdfs.delete_dir_contents(dest)
        logger.info("hdfs dest path dir deleted : %s", dest)
    except Exception as e:
        logger.info("hdfs's content already not exist, dest: %s", dest)

def _detect_file_encoding(file_path):
    if os.path.getsize(file_path) == 0:
        logger.info('File is empty, skipping encoding detection. return default encoding[utf-8]')
        return 'utf-8'

    detector = chardet.universaldetector.UniversalDetector()
    logger.info("detect file encoding: %s", file_path)
    with open(file_path, 'rb') as f:
        for line in f:
            try:
                detector.feed(line)
                if detector.done:
                    break
            except KeyError as e:
                logger.exception("Cannot encode line")
                logger.warning("Cannot encode line : %s", line)
        detector.close()
    result = detector.result['encoding']
    logger.info('encoding result: %s:%s', file_path, result)
    return result

def get_encoding(src, size=10000):
    with open(src, 'rb') as local_file:
        raw_data = local_file.read(size)
        encoding_result = chardet.detect(raw_data)
        logger.info("Encoding : %s", encoding_result['encoding'])
        return encoding_result['encoding']


