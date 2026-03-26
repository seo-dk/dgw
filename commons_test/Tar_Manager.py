import logging
import tarfile
from pathlib import Path

logger = logging.getLogger(__name__)


class TarManager():

    def __init__(self, tar_file_path, local_path):
        self.tar_file_path = str(tar_file_path)
        self.local_path = local_path

    def _check_tar_extension(self, tar_file_path):
        return tar_file_path.endswith(".tar")

    def decompress(self):
        ## if true decompress tar file
        if self._check_tar_extension(self.tar_file_path):
            print("Tar file decompression start!!")
            extract_file_list = []
            with tarfile.open(self.tar_file_path, 'r') as tar:
                for member in tar.getmembers():
                    tar.extract(member, self.local_path)
                    decomp_file_path = Path(self.local_path, member.name)
                    extract_file_list.append(decomp_file_path)
            print("decompress finished...")
            return extract_file_list


if __name__ == "__main__":
    tar_file_path = "/home/samson/user/wjlee/WIFIAAA_LBS_ACCT_2023100513.tar"
    local_path = "/home/samson/user/wjlee/tar_test"

    tm = TarManager(tar_file_path, local_path)
    tm.decompress()
