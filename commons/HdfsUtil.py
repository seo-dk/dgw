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


def get_files(src_path, dest_path):
    start_time = datetime.now()
    cmd = f"hdfs dfs -get -f -t 4 -q 8 {src_path} {dest_path}"

    try:
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True, text=True, check=True)
        logger.info("Command succeeded command: %s, elapsed time: %s", cmd, datetime.now() - start_time)
    except subprocess.CalledProcessError as e:
        raise AirflowException(f"Error occurred: {e.stderr}")


def upload_files(src_path, dest_path):
    logger.info('upload_files from %s to %s: ', src_path, dest_path)

    if not check_path(dest_path):
        make_directory(dest_path)

    # for retry
    put_files(src_path, dest_path)


def check_path(hdfs_path):
    result = subprocess.run(['hdfs', 'dfs', '-test', '-e', hdfs_path], capture_output=True)
    return result.returncode == 0


def is_file_path(hdfs_path):
    result = subprocess.run(['hdfs', 'dfs', '-test', '-f', hdfs_path], capture_output=True)
    return result.returncode == 0


def get_list_count(hdfs_path):
    start_time = datetime.now()
    cmd = f"hdfs dfs -count {hdfs_path}"

    try:
        result = subprocess.run(cmd, capture_output=True, shell=True, text=True, check=True)
        output = result.stdout.strip()
        output_list = output.split()

        if output_list:
            item_count = output_list[1]
            return int(item_count)

        logger.info("Command succeeded command: %s, elapsed time: %s", cmd, datetime.now() - start_time)
    except subprocess.CalledProcessError as e:
        raise AirflowException(f"Error occurred: {e.stderr}")


def make_directory(hdfs_path):
    start_time = datetime.now()
    cmd = f"hdfs dfs -mkdir -p {hdfs_path}"

    try:
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True, text=True, check=True)
        logger.info("Command succeeded command: %s, elapsed time: %s", cmd, datetime.now() - start_time)
    except subprocess.CalledProcessError as e:
        raise AirflowException(f"Error occurred: {e.stderr}")


def get_file_list(input_path):
    cmd = f'hdfs dfs -ls {input_path} | grep -v "_SUCCESS"'
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True, check=True)
        logger.info(f"Command succeeded command: {cmd}")
        files = [
            line.split()[-1]
            for line in result.stdout.splitlines()
            if not line.startswith("Found")
        ]
        return files
    except subprocess.CalledProcessError as e:
        raise Exception(f"Error occurred: {e.stderr}")


def get_file_list_with_ssh(ssh, input_path):
    if ssh is None:
        raise Exception("ssh is none, cannot get file list")

    cmd = f'/bin/bash -l -c "hdfs dfs -ls {input_path}"'
    logger.info(f"command : {cmd}")
    _, stdout, stderr = ssh.exec_command(cmd)
    output = stdout.read().decode().strip()
    err = stderr.read().decode().strip()
    exit_status = stdout.channel.recv_exit_status()

    if exit_status != 0:
        raise Exception(f"Error occurred: {err or output}")

    files = []
    for line in output.splitlines():
        if line.startswith("Found"):
            continue
        parts = line.split()
        if not parts:
            continue
        path = parts[-1]
        if path.endswith("/_SUCCESS") or path.endswith("_SUCCESS"):
            continue
        files.append(path)
    return files


def put_files(local_parquet_path, hdfs_path):
    start_time = datetime.now()
    cmd = ["hdfs", "dfs", "-put", "-f", local_parquet_path, hdfs_path]
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
        logger.info(
            "Command succeeded. Command: %s, Elapsed time: %s, Output: %s",
            " ".join(cmd),
            datetime.now() - start_time,
            result.stdout.strip(),
        )
    except subprocess.CalledProcessError as e:
        logger.error(
            "Command failed. Command: %s, Elapsed time: %s, Error: %s",
            " ".join(cmd),
            datetime.now() - start_time,
            e.stderr.strip() if e.stderr else "No stderr output",
        )
        raise AirflowException(f"Error occurred while executing: {' '.join(cmd)}\nError: {e.stderr.strip() if e.stderr else 'Unknown error'}")


def remove_files(hdfs_path):
    start_time = datetime.now()
    cmd = f"hdfs dfs -rm -r -f -skipTrash {hdfs_path}/*"

    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True, text=True, check=True)
    logger.info("Command succeeded command: %s, elapsed time: %s", cmd, datetime.now() - start_time)
    return result.returncode == 0


def remove_diretory(hdfs_path):
    start_time = datetime.now()
    cmd = f"hdfs dfs -rm -r -f -skipTrash {hdfs_path}"

    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True, text=True, check=True)
    logger.info("Command succeeded command: %s, elapsed time: %s", cmd, datetime.now() - start_time)
    return result.returncode == 0

def is_file_check_with_ssh(ssh,hdfs_path):
    if ssh is None:
        logger.info("ssh is none, cannot check file path")
        return
    cmd = f'/bin/bash -l -c "hdfs dfs -test -f {hdfs_path}"; echo $?'
    logger.info(f"command : {cmd}")
    stdin, stdout, stderr = ssh.exec_command(cmd)
    result = stdout.read().decode().strip()
    err = stderr.read().decode().strip()
    return result == '0'

def is_dir_check_with_ssh(ssh,hdfs_path):
    if ssh is None:
        logger.info("ssh is none, cannot check file path")
        return
    cmd = f'/bin/bash -l -c "hdfs dfs -test -d {hdfs_path}"; echo $?'
    logger.info(f"command : {cmd}")
    stdin, stdout, stderr = ssh.exec_command(cmd)
    result = stdout.read().decode().strip()
    return result == '0'