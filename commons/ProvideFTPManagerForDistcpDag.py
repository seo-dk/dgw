import ftplib
import logging
import os
import re
import subprocess
from datetime import datetime
from ftplib import FTP
import time
import polars as pl
import shutil
import paramiko
from datetime import datetime
from airflow import AirflowException
from airflow.models import Variable

from commons.MetaInfoHook import CollectProvideInfo
from commons.ProvideHistory import ProvideHistory
from commons.ProvideKafkaSend import ProvideKafkaSend
from commons.SftpConnector import SftpConnector
from commons.FtpConnector import FtpConnector
from commons.MetaInfoHook import InterfaceInfo
from commons.Util import Util
from commons.ProvideMetaInfoHook import ProvideMetaInfoHook, ProvideInterface

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))


# collect interace that finished distcp and provided data using sftp/ftp
class ProvideFTPManagerForDistcpDag:
    def __init__(self, collect_interface_info: InterfaceInfo, provide_info: ProvideInterface, hdfs_dest_path, retry, version):
        self.provide_meta_info_hook = ProvideMetaInfoHook()
        self.collect_interface_info = collect_interface_info
        self.provide_interface = provide_info
        # hdfs_dest_path is the path where collection was completed
        self.hdfs_dest_path = hdfs_dest_path
        self._init_collected_vars()
        self.src_files = []
        self.dest_file_paths = []

        self.retry = False
        if retry:
            self.retry = True

        self.version = version

        self.source_path_is_file = self._check_source_path_is_file()

    def _init_collected_vars(self):
        self.partitions = None
        self.partitions_dict = self._extract_partitions(self.hdfs_dest_path)
        self.provide_target = self.provide_meta_info_hook.get_provide_target(self.provide_interface.target_id)
        self.src_path = os.path.join('/data', self.hdfs_dest_path.lstrip('/'))
        logger.info('src_path: %s', self.src_path)

    def _check_source_path_is_file(self):
        _, extension = os.path.splitext(self.hdfs_dest_path)
        logger.info("self.hdfs_dest_path: %s", self.hdfs_dest_path)

        if extension:
            self.src_files.append(os.path.basename(self.src_path))
            self.src_path = os.path.dirname(self.src_path)
            logger.info('extension: %s. , src_path %s', extension, self.src_path)
            return True
        else:
            return False

    def _prepare_src_paths(self):
        logger.info('Preparing source')

        # src_file is multiple file
        if not self.source_path_is_file:
            for f in os.listdir(self.src_path):
                if os.path.isfile(os.path.join(self.src_path, f)):
                    os.remove(os.path.join(self.src_path, f)) if f == '_SUCCESS' else self.src_files.append(f)

        logger.info('src_file.size: %s', len(self.src_files))

    def _prepare_dest_paths(self):
        target_dir = self.provide_interface.proto_info_json.target_dir
        target_file = self.provide_interface.proto_info_json.target_file

        self.formatted_target_dir = self._get_formatted_target_dir(self.partitions_dict, target_dir)

        if self.version == "2" or self.version == "3":
            if not self.provide_interface.proto_info_json.use_origin_path:
                self.formatted_target_dir = os.path.join(self.formatted_target_dir, "v3")
                logger.info("for version2, formatted_target_dir: %s", self.formatted_target_dir)

        # provide target file is single file
        if target_file and target_file.strip():
            # source_file option = original filename does not have a date pattern, partitoins_dt must be used
            if self.provide_interface.proto_info_json.source_file:
                src_dt = self.partitions_dict.get('dt') or self.partitions_dict.get('ym')
                formatted_target_file = self._get_formatted_target_file(self.partitions_dict, src_dt, target_file)
                dest_file_path = os.path.join(self.formatted_target_dir, formatted_target_file)
            else:
                formatted_target_file = self._get_formatted_target_file(self.partitions_dict, self.src_files[0], target_file)
                dest_file_path = os.path.join(self.formatted_target_dir, formatted_target_file)

            logger.info("target_dir: %s, formatted_target_dir: %s, target_file: %s, formatted_target_file: %s", target_dir,
                        self.formatted_target_dir, target_file, formatted_target_file)
            self.dest_file_paths.append(dest_file_path)

        # provide target file is multiple files
        else:
            for src_file_path in self.src_files:
                formatted_target_file = src_file_path
                logger.info("target_dir: %s, formatted_target_dir: %s, target_file: %s, formatted_target_file: %s",
                            target_dir, self.formatted_target_dir, target_file, formatted_target_file)

                dest_file_path = os.path.join(self.formatted_target_dir, formatted_target_file)
                self.dest_file_paths.append(dest_file_path)

        logger.info('dest_file_paths: %s' % self.dest_file_paths)

    def _download(self):
        logger.info('downloading files...')
        start_time = datetime.now()

        if os.path.exists(self.src_path):
            shutil.rmtree(self.src_path)
            logger.info("local path removed for retry: %s", self.src_path)

        os.makedirs(self.src_path, exist_ok=True)


        if self.source_path_is_file:
            cmd = f"hdfs dfs -get -f {self.hdfs_dest_path} {self.src_path}/"
        else:
            cmd = f"hdfs dfs -get -f {self.hdfs_dest_path.rstrip('/')}/* {self.src_path}"

        try:
            result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True, text=True, check=True)
            logger.info("Command succeeded: %s, cmd: %s, elapsed time: %s", result.stdout, cmd, datetime.now() - start_time)
        except subprocess.CalledProcessError as e:
            raise AirflowException(f"Error occurred: {e.stderr}")

    def _upload(self):
        if self.provide_interface.protocol_cd == 'SFTP':
            self._upload_via_sftp()
        elif self.provide_interface.protocol_cd == 'FTP':
            self._upload_via_ftp()
        else:
            raise Exception('undefined protocol_cd')

    def _upload_via_ftp(self):
        def upload_directory(ftp, local_dir, remote_dir):
            if not _is_ftp_remote_path_exists(ftp, remote_dir):
                ftp.mkd(remote_dir)
                logger.info("mkdir %s", remote_dir)

            for item in os.listdir(local_dir):
                local = os.path.join(local_dir, item)
                remote = os.path.join(remote_dir, item)

                if os.path.isfile(local):
                    with open(local, 'rb') as f:
                        ftp.storbinary(f'STOR {remote}', f)
                else:
                    upload_directory(ftp, local, remote)

        def _is_ftp_remote_path_exists(ftp, path):
            try:
                ftp.cwd(path)
                logger.info("current pwd: %s", path)
                return True
            except ftplib.error_perm:
                return False

        try:
            logger.info('Uploading via ftp')
            with FtpConnector(self.provide_target.target_info_json, self.provide_interface.proto_info_json.proxy_setting) as ftp:
                self._check_file(ftp)

                if self.provide_interface.proto_info_json.rename_uploaded_file:
                    self._upload_file_using_tmp(ftp)
                else:
                    if len(self.src_files) == 1:
                        src_file_path = os.path.join(self.src_path, self.src_files[0])

                        if not self._is_ftp_remote_path_exists(ftp, self.formatted_target_dir):
                            logger.info("_ftp_remote_path_exists: False")
                            self._create_ftp_remote_path(ftp, self.formatted_target_dir)

                        with open(src_file_path, 'rb') as f:
                            ftp.storbinary(f'STOR {self.dest_file_paths[0]}', f)

                        logger.info("successfully uploaded via ftp, src_path: %s -> dest_path: %s", src_file_path, self.dest_file_paths[0])
                        self._send_noti(self.dest_file_paths[0])
                    else:
                        upload_directory(ftp, self.src_path, self.formatted_target_dir)
                        logger.info("successfully uploaded via ftp, src_path: %s -> dest_path: %s", self.src_path, self.formatted_target_dir)
                        self._send_noti(self.formatted_target_dir)

                if self.provide_interface.proto_info_json.create_chk_file:
                    self._create_chk_file(ftp)

        except Exception as e:
            raise Exception(
                f"failed to upload via ftp: src_path: {self.src_path}, self.formatted_target_dir: {self.formatted_target_dir}, self.dest_file_paths: {self.dest_file_paths}") from e

    def _upload_via_sftp(self):
        def upload_directory(sftp, local_dir, remote_dir):
            try:
                sftp.chdir(remote_dir)
                logger.info("current pwd: %s", remote_dir)
            except IOError:
                logger.info("_sftp_remote_path_exists: False")
                self._create_sftp_remote_path(sftp, remote_dir)
                logger.info("mkdir %s", remote_dir)
                sftp.chdir(remote_dir)

            for item in os.listdir(local_dir):
                local = os.path.join(local_dir, item)
                remote = os.path.join(remote_dir, item)

                if os.path.isfile(local):
                    sftp.put(local, remote)
                else:
                    upload_directory(sftp, local, remote)

        try:
            logger.info('Uploading via sftp')
            with SftpConnector(self.provide_target.target_info_json, self.provide_interface.proto_info_json.proxy_setting) as sftp:
                self._check_file(sftp)

                if self.provide_interface.proto_info_json.rename_uploaded_file:
                    self._upload_file_using_tmp(sftp)
                else:
                    src_file_path = ""
                    if self.source_path_is_file:
                        src_file_path = os.path.join(self.src_path, self.src_files[0])
                    elif self.provide_interface.proto_info_json.source_file and len(self.dest_file_paths) == 1:
                        src_file_path = os.path.join(self.src_path, self.provide_interface.proto_info_json.source_file)
                    elif len(self.src_files) == 1 and len(self.dest_file_paths) == 1:
                        src_file_path = os.path.join(self.src_path, self.src_files[0])
                    else:
                        upload_directory(sftp, self.src_path, self.formatted_target_dir)
                        logger.info("successfully uploaded via sftp, src_path: %s -> dest_path: %s", self.src_path, self.formatted_target_dir)
                        self._send_noti(self.formatted_target_dir)

                    if src_file_path:
                        logger.info('src_file_path: %s, dest_file_paths: %s', src_file_path, self.dest_file_paths[0])
                        dest_file_dir = os.path.dirname(self.dest_file_paths[0])

                        if not self._is_sftp_remote_path_exists(sftp, dest_file_dir):
                            logger.info("_sftp_remote_path_exists: False")
                            self._create_sftp_remote_path(sftp, dest_file_dir)

                        sftp.put(src_file_path, self.dest_file_paths[0])
                        self._send_noti(self.dest_file_paths[0])

                if self.provide_interface.proto_info_json.create_chk_file:
                    self._create_chk_file(sftp)

        except Exception as e:
            raise Exception(
                f"failed to upload via sftp: src_path: {self.src_path}, self.formatted_target_dir: {self.formatted_target_dir}, self.dest_file_paths: {self.dest_file_paths}") from e

    def _is_ftp_remote_path_exists(self, ftp, target_path):
        try:
            ftp.cwd('/')
            ftp.cwd(target_path)
            return True
        except:
            return False

    def _create_ftp_remote_path(self, ftp, target_path):
        dir_path = target_path
        logger.info("dir_path: %s", dir_path)

        ftp.cwd('/')
        parts = dir_path.split('/')

        for part in parts:
            if part:
                try:
                    ftp.cwd(part)
                    logger.info("ftp.pwd: %s", ftp.pwd())
                except:
                    ftp.mkd(part)
                    logger.info("mkdir %s", part)
                    ftp.cwd(part)
                    logger.info("ftp cdw and current path: %s", ftp.pwd())

    def _is_sftp_remote_path_exists(self, sftp, target_path):
        try:
            sftp.chdir('/')
            sftp.stat(target_path)
            return True
        except:
            return False

    def _create_sftp_remote_path(self, sftp, target_path):
        dir_path = target_path
        logger.info("dir_path: %s", dir_path)

        sftp.chdir('/')
        parts = dir_path.split('/')

        for part in parts:
            if part:
                try:
                    sftp.chdir(part)
                    logger.info("sftp chdir and current path: %s", sftp.getcwd())
                except:
                    sftp.mkdir(part)
                    logger.info("sftp mkdir %s", part)
                    sftp.chdir(part)
                    logger.info("sftp mkdir and current path: %s", sftp.getcwd())

    def _prepare_to_upload(self):
        self._prepare_src_paths()
        self._prepare_dest_paths()

    def _send_noti(self, dest_path):
        provide_history = ProvideHistory(self.collect_interface_info, self.provide_interface, self.provide_target, dest_path, self.retry)
        provide_kafka = ProvideKafkaSend()
        provide_kafka.send_to_kafka(provide_history.provide_history_data)
        provide_kafka.close()

    def _upload_file_using_tmp(self, ftp):
        directory = os.path.dirname(self.dest_file_paths[0])
        file_name, origin_extension = os.path.splitext(os.path.basename(self.dest_file_paths[0]))
        tmp_file_path = os.path.join(directory, file_name) + ".tmp"
        logger.info("start upload tmp file, file_name : %s", tmp_file_path)

        download_file = os.path.join(self.src_path, self.src_files[0])
        logger.info("download_file path : %s", download_file)

        if self.provide_interface.protocol_cd == "FTP":
            if not self._is_ftp_remote_path_exists(ftp, self.formatted_target_dir):
                logger.info("_ftp_remote_path_exists: False")
                self._create_ftp_remote_path(ftp, self.formatted_target_dir)

            before_upload_size = os.path.getsize(download_file)
            logger.info("start upload tmp file, file_name : %s", tmp_file_path)
            with open(download_file, 'rb') as f:
                ftp.storbinary(f'STOR {tmp_file_path}', f)
            after_upload_size = ftp.size(self.dest_file_paths[0])
            if before_upload_size == after_upload_size:
                ftp.rename(tmp_file_path, self.dest_file_paths[0])
                logger.info("tmp upload finish, change to origin file name : %s", self.dest_file_paths[0])

        elif self.provide_interface.protocol_cd == "SFTP":
            if not self._is_sftp_remote_path_exists(ftp, self.formatted_target_dir):
                logger.info("_sftp_remote_path_exists: False")
                self._create_sftp_remote_path(ftp, self.formatted_target_dir)

            try:
                ftp.put(download_file, tmp_file_path, confirm=True)
                ftp.rename(tmp_file_path, self.dest_file_paths[0])
                logger.info("tmp upload finish, change to origin file name : %s", self.dest_file_paths[0])
            except Exception:
                raise Exception("put and rename sftp file failed - download_file: %s, dest_file_paths[0]", download_file, self.dest_file_paths[0])

    def _check_file(self, ftp):
        tmp_filename = None
        dest_chk_file = None

        if self.provide_interface.proto_info_json.create_chk_file:
            extension = "." + self.provide_interface.proto_info_json.chk_file_extension
            dest_chk_file = self.dest_file_paths[0].rsplit('.', 1)[0] + extension
            tmp_filename = os.path.basename(dest_chk_file)

        elif self.provide_interface.proto_info_json.rename_uploaded_file:
            tmp_filename = os.path.basename(self.dest_file_paths[0])

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
        dest_chk_path = self.dest_file_paths[0].rsplit('.', 1)[0] + extension
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
                ftp.cwd(os.path.dirname(self.dest_file_paths[0]))
                ftp.delete(tmp_filename)
                logger.info("tmp_filename removed: %s", tmp_filename)
            except Exception as e:
                if '550' in str(e):
                    logger.info("tmp_filename does not exist: %s", tmp_filename)
                else:
                    logger.exception("An error occurred while trying to remove the FTP file.")

        elif self.provide_interface.protocol_cd == "SFTP":
            tmp_file_path = os.path.join(os.path.dirname(self.dest_file_paths[0]), tmp_filename)
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

    def _get_formatted_target_file(self, partitions_dict, src_file_nm, target_file):
        if self.version == "2":
            dt = partitions_dict.get("dt") or partitions_dict.get("ym") or ''
            hh = partitions_dict.get("hh", '')

            src_dt = dt + hh
        else:
            regex = r"((?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12][0-9]|3[01]))((?:[01][0-9]|2[0-3])?(?:[0-5][0-9]))?"
            match = re.search(regex, src_file_nm)

            src_dt = ""

            if match:
                src_dt = match.group()
            # case: distcp_batch's origin file has no src_dt
            else:
                dt = partitions_dict.get("dt") or partitions_dict.get("ym") or ''
                hh = partitions_dict.get("hh", '')

                src_dt = dt + hh

        formatted_target_file = Util.get_file_nm(src_dt, target_file)

        return formatted_target_file

    def _get_formatted_target_dir(self, partitions_dict, target_dir):
        logger.info(f"partitions_dict : {partitions_dict}")
        logger.info(f"target_dir : {target_dir}")
        dt = partitions_dict.get("dt")
        hh = partitions_dict.get("hh", '')
        hm = partitions_dict.get("hm", '')
        ym = partitions_dict.get("ym", '')

        if dt is not None:
            partitions_dt = dt + hh + hm
        else:
            partitions_dt = ym

        formatted_target_dir = Util.get_file_nm_by_pattern(partitions_dt, target_dir)
        if self.provide_interface.proto_info_json.staging_dir:
            formatted_target_dir = Util.get_file_nm(partitions_dt, self.provide_interface.proto_info_json.staging_dir)

        return formatted_target_dir

    def _extract_partitions(self, hdfs_dest_path):
        partitions_dict = {}
        dt_match = re.search(r'dt=(\d+)', hdfs_dest_path)
        hh_match = re.search(r'hh=(\d+)', hdfs_dest_path)
        hm_match = re.search(r'hm=(\d+)', hdfs_dest_path)
        ym_match = re.search(r'ym=(\d{6})', hdfs_dest_path)

        if dt_match:
            partitions_dict['dt'] = dt_match.group(1)
        if hh_match:
            partitions_dict['hh'] = hh_match.group(1)
        if hm_match:
            partitions_dict['hm'] = hm_match.group(1)
        if ym_match:
            partitions_dict['ym'] = ym_match.group(1)

        if not partitions_dict and 'staging' in hdfs_dest_path:
            logger.info("hdfs_dest_path has no partitions, so need to create it")
            now = datetime.now()
            partitions_dict['dt'] = now.strftime('%Y%m%d')
            partitions_dict['hh'] = now.strftime('%H')

        return partitions_dict

    def execute(self):
        self._download()
        self._prepare_to_upload()
        self._upload()
        return self.formatted_target_dir
