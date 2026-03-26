import logging
import tarfile
from pathlib import Path

logger = logging.getLogger(__name__)


class TarManager:

    def __init__(self, tar_full_path, local_tar_path):
        self.tar_full_path = str(tar_full_path)
        self.local_tar_path = local_tar_path

    def _is_tar(self, tar_full_path):
        return tar_full_path.endswith(".tar")

    def extract(self):
        if self._is_tar(self.tar_full_path):
            logger.info("extraction started. file: " + self.tar_full_path)
            extract_file_list = []
            with tarfile.open(self.tar_full_path, 'r') as tar:
                for member in tar.getmembers():
                    tar.extract(member, self.local_tar_path)
                    extracted_file_path = Path(self.local_tar_path, member.name)
                    extract_file_list.append(extracted_file_path)
            logger.info("extraction finished. file: " + self.tar_full_path)
            return extract_file_list


if __name__ == "__main__":
    tar_file_path = "/home/samson/user/wjlee/WIFIAAA_LBS_ACCT_2023100513.tar"
    local_path = "/home/samson/user/wjlee/tar_test"

    tm = TarManager(tar_file_path, local_path)
    tm.extract()
