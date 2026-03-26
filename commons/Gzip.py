import logging
import os
import subprocess
import tarfile
import gzip
import shutil

from airflow.models import Variable

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))


class Gzip:
    def _get_tar_gzip_file_list(self, gz_file_nm):
        try:
            process = subprocess.Popen(['tar', '-tf', gz_file_nm], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()

            if process.returncode != 0:
                logger.error(f"Error executing tar : {stderr.decode('utf-8')}")
                return None
            return stdout.decode('utf-8').strip().split("\n")
        except Exception as e:
            logger.error(f"Error : {e}")
            return None

    def _check_compress_list(self, comp_files, filename):
        return any(filename in item for item in comp_files)

    def _decompress_tar_gz_file(self, tar_gz_file_path, local_tmp):
        try:
            with tarfile.open(tar_gz_file_path, "r:gz") as tar:
                tar.extractall(path=local_tmp)
        except Exception as e:
            logger.error(e)

    def check_gz_extension(self, filename):
        _, fileExtension = os.path.splitext(filename)
        return fileExtension == ".gz"

    def check_file_in_targz(self, gzip_paths, local_tmp, file_list_info):
        for gzip_path in gzip_paths:
            flag = False
            # get zip file list
            gzip_file_nm = os.path.basename(gzip_path)
            if self.check_gz_extension(gzip_file_nm):
                gzip_file_list = self._get_tar_gzip_file_list(gzip_path)
                for file_info in file_list_info:
                    # check if filename is in the gzipFileList
                    if self._check_compress_list(gzip_file_list, file_info.file_proto_info.file_nm):
                        # if exist decompressfrt
                        self._decompress_tar_gz_file(gzip_path, local_tmp)
                        flag = True
                        break
                if flag:
                    break

    def decompress_gz_file(self, gz_full_path):
        logger.info("start decompress")
        output_file_path = os.path.splitext(gz_full_path)[0]
        logger.info("decompress file name : %s", output_file_path)
        with gzip.open(gz_full_path, 'rt') as gz_file, open(output_file_path, 'w') as output:
            shutil.copyfileobj(gz_file, output)
        return output_file_path