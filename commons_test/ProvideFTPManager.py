import datetime
import ftplib
import logging
import os
import re
import subprocess
import time
from datetime import datetime
from ftplib import FTP

import paramiko
from airflow.exceptions import AirflowException
from airflow.models import Variable

from commons_test.MetaInfoHook import CollectProvideInfo
from commons_test.ProvideMetaInfoHook import ProvideMetaInfoHook
from commons_test.ProvideHistory import ProvideHistory
from commons_test.ProvideKafkaSend import ProvideKafkaSend
from commons_test.SftpConnector import SftpConnector
from commons_test.FtpConnector import FtpConnector
from commons_test.Util import Util
from commons_test import FileUtil

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))


class ProvideFTPManager:
    def __init__(self, context, collect_interface_info=None, source_provide_file_info: CollectProvideInfo = None, src_path=None, retry=None):
        self.collect_interface_info = collect_interface_info

        self.collect_provide_info = source_provide_file_info

        if source_provide_file_info and src_path:
            self._init_collected_vars(source_provide_file_info, src_path)
        else:
            self._init_context_vars(context)

        self.retry = False

        if retry:
            self.retry = True

    # trigger
    def _init_context_vars(self, context):
        logger.info(f'_init_context_vars: context {context}')

        dag_run = context['dag_run']
        self.interface_id = dag_run.conf.get('interface_id')
        self.src_file_path = dag_run.conf.get('filepath')
        self.src_file_nm = dag_run.conf.get('filenm')
        self.partitions = dag_run.conf.get('partitions')
        self.execution_date = context['execution_date']
        logger.info("dag_run.conf: %s", dag_run.conf)

        self._get_meta_info()
        self._set_download_path()

        partitions_dict = self._extract_partitions()
        self.dest_path = self._get_dest_path(partitions_dict)
        logger.info("self.dest_path: %s", self.dest_path)

    # collect and provide
    def _init_collected_vars(self, source_provide_file_info: CollectProvideInfo, src_path):
        self.interface_id = source_provide_file_info.provide_interface_id
        self.src_file_path = os.path.dirname(src_path)
        self.src_file_nm = os.path.basename(src_path)
        self.partitions = None
        logger.info("interface_id: %s, src_file_path: %s, src_file_nm: %s", self.interface_id, self.src_file_path, self.src_file_nm)

        self._get_meta_info()
        self._set_download_path()

        partitions_dict = self._extract_partitions()
        self.dest_path = self._get_dest_path(partitions_dict)
        logger.info("dest_path: %s", self.dest_path)

    def _get_meta_info(self):
        provide_meta_info_hook = ProvideMetaInfoHook()
        self.provide_interface = provide_meta_info_hook.get_provide_interface(self.interface_id)
        self.provide_target = provide_meta_info_hook.get_provide_target(self.provide_interface.target_id)

    def _set_download_path(self):
        self.download_path = os.path.join('/data/idcube_in', self._extract_local_path(self.src_file_path))
        self.download_file = os.path.join(self.download_path, self.src_file_nm)
        logger.info('download_path: %s, download_file: %s', self.download_path, self.download_file)

    def execute(self):
        self._check_size()
        self._download()
        self._transform()
        self._upload()
        self._send_noti()
        return self.dest_path

    def _transform(self):
        if self.provide_interface.proto_info_json.change_delimiter:
            logger.error("start changing delimiter")
            collect_del = self.collect_provide_info.get_collect_delimiter
            provide_del = self.collect_provide_info.get_provide_delimiter
            FileUtil.replace_delimiter(self.download_file, collect_del, provide_del)

    def _get_dest_path(self, partitions_dict):
        target_dir = self.provide_interface.proto_info_json.target_dir
        target_file = self.provide_interface.proto_info_json.target_file

        # test
        formatted_target_dir = self._get_formatted_target_dir(partitions_dict, target_dir) + "/test"

        if target_file and target_file.strip():
            formatted_target_file = self._get_formatted_target_file(self.src_file_nm, target_file)
        else:
            formatted_target_file = self.src_file_nm

        logger.info("self.src_file_nm: %s, target_dir: %s, formatted_target_dir: %s, target_file: %s, formatted_target_file: %s",
                    self.src_file_nm, target_dir, formatted_target_dir, target_file, formatted_target_file)

        return os.path.join(formatted_target_dir, formatted_target_file)

    def _get_formatted_target_file(self, src_file_nm, target_file):
        if self.provide_interface.proto_info_json.preserve_filename:
            return src_file_nm

        regex = r"((?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01]))((?:[01][0-9]|2[0-3])?(?:[0-5][0-9]))?"
        match_1 = re.search(regex, src_file_nm)

        # kma_weather_1h
        regex = r"\b(\d{2}[01]\d[0-3]\d[0-2]\d45(?:\.\w+)+)\b"
        match_2 = re.search(regex, src_file_nm)

        src_dt = ""

        if match_1:
            src_dt = match_1.group()
        elif match_2:
            src_dt = match_2.group()

        formatted_target_file = Util.get_file_nm(src_dt, target_file)

        return formatted_target_file

    def _get_formatted_target_dir(self, partitions_dict, target_dir):
        dt = partitions_dict.get("dt")
        hh = partitions_dict.get("hh", '')

        partitions_dt = dt + hh

        formatted_target_dir = Util.get_file_nm(partitions_dt, target_dir)

        return formatted_target_dir

    def _check_size(self):
        start_time = datetime.now()
        src_path = os.path.join(self.src_file_path, self.src_file_nm)
        cmd = f"hdfs dfs -ls {src_path}"
        try:
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True, check=True)
            logger.info("Command succeeded: %s, elapsed time: %s", result.stdout, datetime.now() - start_time)
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to execute command: {e.stderr}")
            raise AirflowException(f"Failed to check file size. cmd: {cmd}")

    def _download(self):

        os.makedirs(self.download_path, exist_ok=True)
        start_time = datetime.now()
        logger.info('Download path ensured: %s', self.download_path)

        src_path = os.path.join(self.src_file_path, self.src_file_nm)
        cmd = f"hdfs dfs -get -f {src_path} {self.download_path}"

        try:
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True, check=True)
            logger.info("Command succeeded: %s, elapsed time: %s", result.stdout, datetime.now() - start_time)
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to execute command: {e.stderr}")
            raise AirflowException(f"Failed to get file. cmd: {cmd}")

    def _upload_via_ftp(self):
        try:
            with FtpConnector(self.provide_target.target_info_json, self.provide_interface.proto_info_json.proxy_setting) as ftp:
                self._check_file(ftp)
                if not self._ftp_remote_path_exists(ftp):
                    logger.info("_ftp_remote_path_exists: False")
                    self._ftp_create_remote_path(ftp)
                if self.provide_interface.proto_info_json.rename_uploaded_file:
                    self._upload_file_using_tmp(ftp)
                else:
                    with open(self.download_file, 'rb') as f:
                        ftp.storbinary(f'STOR {self.dest_path}', f)
                        logger.info("successfully uploaded via ftp, dest_path: %s", self.dest_path)

                        logger.info(f'self.provide_interface.proto_info_json: {self.provide_interface.proto_info_json.create_chk_file}')

                if self.provide_interface.proto_info_json.create_chk_file:
                    self._create_chk_file(ftp)
        except Exception as e:
            raise Exception("failed to upload via ftp: dest_path: %s" % self.dest_path) from e

    def _upload_via_sftp(self):
        try:
            with SftpConnector(self.provide_target.target_info_json, self.provide_interface.proto_info_json.proxy_setting) as sftp:
                self._check_file(sftp)

                if not self._sftp_remote_path_exists(sftp):
                    logger.info("_sftp_remote_path_exists: False")
                    self._sftp_create_remote_path(sftp)
                if self.provide_interface.proto_info_json.rename_uploaded_file:
                    self._upload_file_using_tmp(sftp)
                else:
                    sftp.put(self.download_file, self.dest_path)
                logger.info("successfully uploaded via sftp, dest_path: %s", self.dest_path)
                if self.provide_interface.proto_info_json.create_chk_file:
                    self._create_chk_file(sftp)

        except Exception as e:
            raise Exception("failed to upload via sftp: dest_path: %s" % self.dest_path)

    def _upload(self):
        if self.provide_interface.protocol_cd == 'SFTP':
            self._upload_via_sftp()
        elif self.provide_interface.protocol_cd == 'FTP':
            self._upload_via_ftp()
        else:
            raise Exception('undefined protocol_cd')

    def _ftp_remote_path_exists(self, ftp):
        try:
            ftp.cwd('/')
            ftp.cwd(self.dest_path)
            return True
        except:
            return False

    def _ftp_create_remote_path(self, ftp):
        ftp.cwd('/')
        parts = os.path.dirname(self.dest_path).split('/')

        for part in parts:
            if not part:
                continue

            try:
                ftp.cwd(part)
                logger.info("Changed to existing directory: %s", part)
            except ftplib.error_perm:
                try:
                    ftp.mkd(part)
                    logger.info("Created directory: %s", part)
                    ftp.cwd(part)
                except ftplib.error_perm as e:
                    logger.error("Error creating directory %s: %s", part, e)
                    raise

    def _sftp_remote_path_exists(self, sftp):
        try:
            sftp.chdir('/')
            sftp.stat(self.dest_path)
            return True
        except:
            return False

    def _sftp_create_remote_path(self, sftp):
        dir_path = os.path.dirname(self.dest_path)
        sftp.chdir('/')
        parts = dir_path.split('/')

        for part in parts:
            if not part:
                continue

            try:
                sftp.chdir(part)
            except IOError:
                try:
                    sftp.mkdir(part)
                    logger.info("Created directory: %s", part)
                    sftp.chdir(part)
                except IOError as e:
                    logger.error("Error creating directory %s: %s", part, e)
                    raise

    def _extract_local_path(self, path):
        pattern = r'.*(db=[^/]+/tb=[^/]+/.*)'
        match = re.search(pattern, path)
        if match:
            return match.group(1)
        else:
            return None

    def _extract_partitions(self):
        if self.partitions:
            return {key: value for data in self.partitions for key, value in data.items()}
        else:
            return self._extract_partitions_from_path(self.src_file_path)

    def _extract_partitions_from_path(self, path):
        partitions = {}
        matches = re.findall(r'(dt|hh)=(\d+)', path)
        for key, value in matches:
            partitions[key] = value

        logger.info("partitions_dict: %s", partitions)
        return partitions

    def _send_noti(self):
        provide_history = ProvideHistory(self.collect_interface_info, self.provide_interface, self.provide_target, self.dest_path, self.retry)
        provide_kafka = ProvideKafkaSend()
        provide_kafka.send_to_kafka(provide_history.provide_history_data)
        provide_kafka.close()

    def _upload_file_using_tmp(self, ftp):
        directory = os.path.dirname(self.dest_path)
        file_name, origin_extension = os.path.splitext(os.path.basename(self.dest_path))
        tmp_file_path = os.path.join(directory, file_name) + ".tmp"
        logger.info("start upload tmp file, file_name : %s", tmp_file_path)

        if self.provide_interface.protocol_cd == "FTP":
            before_upload_size = os.path.getsize(self.download_file)
            logger.info("start upload tmp file, file_name : %s", tmp_file_path)
            with open(self.download_file, 'rb') as f:
                ftp.storbinary(f'STOR {tmp_file_path}', f)
            after_upload_size = ftp.size(self.dest_path)
            if before_upload_size == after_upload_size:
                ftp.rename(tmp_file_path, self.dest_path)
                logger.info("tmp upload finish, change to origin file name : %s", self.dest_path)

        elif self.provide_interface.protocol_cd == "SFTP":
            try:
                ftp.put(self.download_file, tmp_file_path, confirm=True)
                ftp.rename(tmp_file_path, self.dest_path)
                logger.info("tmp upload finish, change to origin file name : %s", self.dest_path)
            except Exception:
                raise Exception("Error SFTP get, file : %s" % self.download_file)

    def _check_file(self, ftp):
        tmp_filename = None
        dest_chk_file = None

        if self.provide_interface.proto_info_json.create_chk_file:
            extension = "." + self.provide_interface.proto_info_json.chk_file_extension
            dest_chk_file = self.dest_path.rsplit('.', 1)[0] + extension
            tmp_filename = os.path.basename(dest_chk_file)

        elif self.provide_interface.proto_info_json.rename_uploaded_file:
            tmp_filename = os.path.basename(self.dest_path)

        if tmp_filename:
            if self.provide_interface.protocol_cd == "FTP":
                self._remove_tmp_file(ftp, tmp_filename)
            elif self.provide_interface.protocol_cd == "SFTP":
                if dest_chk_file:
                    self._remove_chk_file(ftp, dest_chk_file)
                else:
                    self._remove_tmp_file(ftp, tmp_filename)

    def _create_chk_file(self, ftp):
        extension = "." + self.provide_interface.proto_info_json.chk_file_extension
        dest_chk_path = self.dest_path.rsplit('.', 1)[0] + extension
        local_chk_path = f'/data/tmp/{os.path.basename(dest_chk_path)}'

        with open(local_chk_path, 'wb'):
            pass

        if self.provide_interface.protocol_cd == "FTP":
            with open(local_chk_path, 'rb') as chk_file:
                ftp.storbinary(f'STOR {dest_chk_path}', chk_file)
        elif self.provide_interface.protocol_cd == "SFTP":
            ftp.put(local_chk_path, dest_chk_path)

        logger.info("successfully uploaded chk file: %s", dest_chk_path)

        os.remove(local_chk_path)

    def _remove_tmp_file(self, ftp, tmp_filename):
        if self.provide_interface.protocol_cd == "FTP":
            try:
                ftp.cwd(os.path.dirname(self.dest_path))
                ftp.delete(tmp_filename)
                logger.info("tmp_filename removed: %s", tmp_filename)
            except Exception as e:
                if '550' in str(e):
                    logger.info("tmp_filename does not exist: %s", tmp_filename)
                else:
                    logger.exception("An error occurred while trying to remove the FTP file.")

        elif self.provide_interface.protocol_cd == "SFTP":
            tmp_file_path = os.path.join(os.path.dirname(self.dest_path), tmp_filename)
            try:
                ftp.remove(tmp_file_path)
                logger.info("tmp_filename removed: %s", tmp_filename)
            except FileNotFoundError:
                logger.info("tmp_filename does not exist: %s", tmp_filename)
            except Exception:
                logger.exception("An error occurred while trying to remove the SFTP file.")

    def _remove_chk_file(self, ftp, dest_chk_file):
        if self.provide_interface.protocol_cd == "FTP":
            try:
                ftp.delete(dest_chk_file)
                logger.info("chk_file removed: %s", dest_chk_file)
            except Exception as e:
                if '550' in str(e):
                    logger.info("chk_file does not exist: %s", dest_chk_file)
                else:
                    logger.exception("An error occurred while trying to remove the FTP file.")
        elif self.provide_interface.protocol_cd == "SFTP":
            try:
                ftp.remove(dest_chk_file)
                logger.info("chk_file removed: %s", dest_chk_file)
            except FileNotFoundError:
                logger.info("chk_file does not exist: %s", dest_chk_file)
            except Exception:
                logger.exception("An error occurred while trying to remove the SFTP file.")


if __name__ == "__main__":
    def create_test_context():
        conf = {
            "db": "test",
            "filenm": "test_file.dat",
            "filepath": "hdfs:///test/db=test/tb=test/",
            "interface_id": "P-TEST-SFTP-OD-0001",
            "partitions": [
                {
                    "dt": "20240319"
                },
                {
                    "hm": "1125"
                }
            ],
            "tb": "test"
        }

        context = {
            'dag_run': type('DagRun', (object,), {'conf': conf})(),
            'execution_date': datetime.datetime.now()
        }

        return context


    ftp_manager = ProvideFTPManager(create_test_context())
    ftp_manager.execute()

    # ftp_manager.format_path("cms_spc_area_eqp_20240109.csv", '20240109', None)
