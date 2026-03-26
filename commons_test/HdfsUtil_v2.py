import logging
import os
import json
import pathlib
import subprocess
from datetime import datetime
import os
import glob

from airflow.exceptions import AirflowException
from airflow.models import Variable

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))

hadoop_home = '/local/HADOOP'

pattern = [hadoop_home + '/share/hadoop/' + d + '/**/*.jar' for d in ['hdfs', 'common']]
hdfs_cp = ':'.join(file for p in pattern for file in glob.glob(p, recursive=True))

os.environ['CLASSPATH'] = ':'.join([hadoop_home + '/etc/hadoop:', hdfs_cp])
os.environ['LD_LIBRARY_PATH'] = os.getenv('LD_LIBRARY_PATH', '') + ':' + hadoop_home + '/lib/native:'


def hdfs_get_files(src_path, dest_path):
    start_time = datetime.now()
    cmd = f"hdfs dfs -get -f -t 4 -q 8 {src_path} {dest_path}"

    try:
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True, text=True, check=True)
        logger.info("Command succeeded command: %s, elapsed time: %s", cmd, datetime.now() - start_time)
    except subprocess.CalledProcessError as e:
        raise AirflowException(f"Error occurred: {e.stderr}")


def upload_files_to_hdfs(src_path, dest_path):
    logger.info('upload_files from %s to %s: ', src_path, dest_path)

    if not check_hdfs_path(dest_path):
        mkdir_to_hdfs(dest_path)

    # for retry
    put_files_to_hdfs(src_path, dest_path)


def check_hdfs_path(hdfs_path):
    result = subprocess.run(['hdfs', 'dfs', '-test', '-e', hdfs_path], capture_output=True)
    return result.returncode == 0


def check_hdfs_path_is_file(hdfs_path):
    result = subprocess.run(['hdfs', 'dfs', '-test', '-f', hdfs_path], capture_output=True)
    return result.returncode == 0

def hdfs_list_count(hdfs_path):
    start_time = datetime.now()
    cmd = f"hdfs dfs -count {hdfs_path}"

    try:
        result = subprocess.run(cmd, capture_output=True, shell=True, text=True, check=True)
        output = result.stdout.strip()
        return_value = output.split()

        if return_value:
            # item count
            return int(return_value[1])

        logger.info("Command succeeded command: %s, elapsed time: %s", cmd, datetime.now() - start_time)
    except subprocess.CalledProcessError as e:
        raise AirflowException(f"Error occurred: {e.stderr}")

def mkdir_to_hdfs(hdfs_path):
    start_time = datetime.now()
    cmd = f"hdfs dfs -mkdir -p {hdfs_path}"

    try:
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True, text=True, check=True)
        logger.info("Command succeeded command: %s, elapsed time: %s", cmd, datetime.now() - start_time)
    except subprocess.CalledProcessError as e:
        raise AirflowException(f"Error occurred: {e.stderr}")


def put_files_to_hdfs(local_parquet_path, hdfs_path):
    start_time = datetime.now()
    cmd = f"hdfs dfs -put -f {local_parquet_path} {hdfs_path}"

    try:
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True, text=True, check=True)
        logger.info("Command succeeded command: %s, elapsed time: %s", cmd, datetime.now() - start_time)
    except subprocess.CalledProcessError as e:
        raise AirflowException(f"Error occurred: {e.stderr}")


def hdfs_remove_files(hdfs_path):
    start_time = datetime.now()
    cmd = f"hdfs dfs -rm -r -f -skipTrash {hdfs_path}/*"

    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True, text=True, check=True)
    logger.info("Command succeeded command: %s, elapsed time: %s", cmd, datetime.now() - start_time)
    return result.returncode == 0


def hdfs_remove_dir(hdfs_path):
    start_time = datetime.now()
    cmd = f"hdfs dfs -rm -r -f -skipTrash {hdfs_path}"

    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True, text=True, check=True)
    logger.info("Command succeeded command: %s, elapsed time: %s", cmd, datetime.now() - start_time)
    return result.returncode == 0
