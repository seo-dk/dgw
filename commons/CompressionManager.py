import logging
import os
import subprocess
import tarfile
import gzip
import shutil
import zipfile
import zlib
from pathlib import Path

from airflow.models import Variable

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))

class CompressionManager:
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

    def check_gz_extension(self, filename):
        _, fileExtension = os.path.splitext(filename)
        return fileExtension == ".gz"

    def _check_tar_extension(self, tar_full_path):
        return str(tar_full_path).endswith(".tar")

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

    def _decompress_tar_gz_file(self, tar_gz_file_path, local_tmp):
        try:
            with tarfile.open(tar_gz_file_path, "r:gz") as tar:
                tar.extractall(path=local_tmp)
        except Exception as e:
            logger.error(e)
    def decompress_gz_file(self, gz_full_path):
        logger.info("start decompress")
        output_file_path = os.path.splitext(gz_full_path)[0]
        logger.info("decompress file name : %s", output_file_path)
        with gzip.open(gz_full_path, 'rt') as gz_file, open(output_file_path, 'w') as output:
            shutil.copyfileobj(gz_file, output)
        return output_file_path

    def decompress_zip_file(self, local_zip_full_path):
        logger.info("start decompress zip file...")
        output_file_path = os.path.dirname(local_zip_full_path)
        extract_file_full_path_list = []
        try:
            with zipfile.ZipFile(local_zip_full_path, 'r') as zip_ref:
                extract_file_list = zip_ref.namelist()
                logger.info("decompressing zip file...")
                zip_ref.extractall(output_file_path)
                for extract_file in extract_file_list:
                    extract_file_full_path_list.append(os.path.join(output_file_path, extract_file))
            return extract_file_full_path_list
        except Exception as e:
            raise Exception(e)

    def decompress_tar_file(self, tar_full_path, local_tar_dir):
        if self._check_tar_extension(tar_full_path):
            logger.info("extraction started. file: " + str(tar_full_path))
            extract_file_list = []
            with tarfile.open(tar_full_path, 'r') as tar:
                for member in tar.getmembers():
                    tar.extract(member, local_tar_dir)
                    extracted_file_path = Path(local_tar_dir, member.name)
                    extract_file_list.append(extracted_file_path)
            logger.info("extraction finished. file: " + str(local_tar_dir))
            return extract_file_list

    def decompress_gz_with_zlib(self, local_full_path):
        if ".merge" in local_full_path:
            remove_merge_path = local_full_path.replace(".merge", '')
            local_full_path = remove_merge_path

        logger.debug("start decompress gz with zlib.. | file name : %s",local_full_path)
        decompressed_gz_file_full_path = local_full_path.replace(".gz", '')
        try:
            with open(local_full_path, 'rb') as read_file:
                decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
                with open(decompressed_gz_file_full_path, 'wb') as write_file:
                    while True:
                        chunk = read_file.read(1024)
                        if not chunk:
                            break
                        write_file.write(decompressor.decompress(chunk))
                    write_file.write(decompressor.flush())
            logger.debug("finished decompress gz with zlib..")
            return decompressed_gz_file_full_path
        except Exception as e:
            logger.exception("Error decompressing file %s", local_full_path)

