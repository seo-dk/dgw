import logging
import os
from pathlib import Path
from commons_test.MetaInfoHook import InterfaceInfo

logger = logging.getLogger(__name__)


class DelimiterChanger:
    def replace(self, local_tmp, interface_info:InterfaceInfo):
        logger.info("Checking delimiter start....")
        byte_size = 0
        if interface_info.ftp_proto_info.delimiter:
            delimiter = bytes(interface_info.ftp_proto_info.delimiter, "utf-8").decode("unicode_escape")
            to_byte = delimiter.encode("utf-8")
            byte_size = len(to_byte)

        directory, filenm = os.path.split(local_tmp)
        file_basnm, file_extension = os.path.splitext(filenm)
        tmp_out = file_basnm + "_tmp" + file_extension
        tmp_fullpath = Path(directory, tmp_out)

        if byte_size > 1:
            logger.info("Change delimiter started. local_tmp: %s", local_tmp)
            with open(local_tmp, 'r', encoding=interface_info.ftp_proto_info.encode, errors='replace') as f_in:
                with open(tmp_fullpath, 'w', encoding=interface_info.ftp_proto_info.encode, errors='replace') as f_out:
                    for line in f_in:
                        replace_line = line
                        if interface_info.ftp_proto_info.delimiter == "|^|":
                            replace_line = line.replace("|^|", "\036")
                        if interface_info.ftp_proto_info.delimiter == ",|'":
                            replace_line = line.replace(",|'", "\036")
                        if interface_info.ftp_proto_info.delimiter == "\",\"":
                            replace_line = line.replace(",","\036")
                        f_out.write(replace_line)
            if os.path.exists(local_tmp):
                os.remove(local_tmp)
                logger.info('local_tmp removed: %s', local_tmp)

            if os.path.exists(tmp_fullpath):
                logger.info("tmp_fullpath exists: %s", tmp_fullpath)
                os.rename(tmp_fullpath, local_tmp)
                logger.info("renamed. tmp_fullpath -> local_tmp. %s -> %s", tmp_fullpath, local_tmp)


    def replace_for_provide(self, local_path, collect_provide_info):
        logger.info("changing delimiter for provide start....")

        if self.check_gz_extension(local_path):
            logger.info("stop changing delimiter, because target file is gz")
            return

        logger.info("pre_delimiter %s", collect_provide_info.collect_delimiter)
        logger.info("next_delimiter %s", collect_provide_info.provide_delimiter)

        pre_delimiter = collect_provide_info.collect_delimiter.decode("utf-8")
        next_delimiter = collect_provide_info.provide_delimiter.decode("utf-8")

        logger.info('pre_delimiter: %s', pre_delimiter)
        logger.info('next_delimiter: %s', next_delimiter)

        if pre_delimiter.startswith('\\'):
            pre_delimiter = pre_delimiter.encode("utf-8").decode('unicode_escape')

        if next_delimiter.startswith('\\'):
            next_delimiter = next_delimiter.encode("utf-8").decode('unicode_escape')

        logger.info('pre_delimiter: %s', pre_delimiter)
        logger.info('next_delimiter: %s', next_delimiter)

        pre_encode = collect_provide_info.collect_encode.decode("utf-8")
        next_encode = collect_provide_info.provide_encode.decode("utf-8")

        directory, filenm = os.path.split(local_path)
        file_basnm, file_extension = os.path.splitext(filenm)
        tmp_out = file_basnm + "_tmp" + file_extension
        tmp_fullpath = Path(directory, tmp_out)

        logger.info("Change delimiter started. local_path: %s", local_path)
        # with open(local_path, 'r', encoding='EUC-KR', errors='replace') as f_in:
        #     with open(tmp_fullpath, 'w', encoding='EUC-KR', errors='replace') as f_out:
        with open(local_path, 'r', encoding=pre_encode, errors='replace') as f_in:
            with open(tmp_fullpath, 'w', encoding=next_encode, errors='replace') as f_out:
                for line in f_in:
                    replace_line = line

                    if not pre_delimiter == next_delimiter:
                        replace_line = line.replace(pre_delimiter, next_delimiter)

                    f_out.write(replace_line)

            if os.path.exists(local_path):
                os.remove(local_path)
                logger.info('local_path removed: %s', local_path)

            if os.path.exists(tmp_fullpath):
                logger.info("tmp_fullpath exists: %s", tmp_fullpath)
                os.rename(tmp_fullpath, local_path)
                logger.info("renamed. tmp_fullpath -> local_tmp. %s -> %s", tmp_fullpath, local_path)


    def check_gz_extension(self, filename):
        _, fileExtension = os.path.splitext(filename)

        return fileExtension == ".gz"


# interface_info.ftp_proto_info.delimiter = "\\u001e"
# cd = ChangeDelimiter()
# cd.change_delimiter("/home/samson/user/wjlee/test_delimiter.txt","|^|")

if __name__ == "__main__":
    local_path = "/data/test/user/kyunghyun/5G_SS_20250429_20222A33V.csv"
    DelimiterChanger().replace_for_provide(local_path, None)

