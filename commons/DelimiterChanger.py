import logging
import os
from pathlib import Path
from commons.MetaInfoHook import InterfaceInfo

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
                            replace_line = line.replace("\",\"", "\"\036\"")
                        f_out.write(replace_line)
            if os.path.exists(local_tmp):
                os.remove(local_tmp)
                logger.info('local_tmp removed: %s', local_tmp)

            if os.path.exists(tmp_fullpath):
                logger.info("tmp_fullpath exists: %s", tmp_fullpath)
                os.rename(tmp_fullpath, local_tmp)
                logger.info("renamed. tmp_fullpath -> local_tmp. %s -> %s", tmp_fullpath, local_tmp)
# cd = ChangeDelimiter()
# cd.change_delimiter("/home/samson/user/wjlee/test_delimiter.txt","|^|")
