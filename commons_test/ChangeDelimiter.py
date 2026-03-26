import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


class ChangeDelimiter:

    def change_delimiter(self, local_tmp, meta_info):
        logger.info("Checking delimiter start....")
        byte_size = 0
        delimiter = meta_info.ftp_proto_info.delimiter
        if meta_info.ftp_proto_info.delimiter:
            try:
                convert_delimiter = delimiter.encode('latin1').decode('unicode-escape')
            except UnicodeDecodeError:
                logger.error("delimiter convert error.")
                convert_delimiter = delimiter.encode('utf-8')
            to_byte = convert_delimiter.encode("utf-8")
            # logger.info("delimiter: %s",meta_info.FTP_SERVER_INFO.DELIMITER)
            # byte_size = len(meta_info.FTP_SERVER_INFO.DELIMITER)
            byte_size = len(to_byte)

        directory, filenm = os.path.split(local_tmp)
        file_basnm, file_extension = os.path.splitext(filenm)
        tmp_out = file_basnm + "_tmp" + file_extension
        tmp_fullpath = Path(directory, tmp_out)

        if byte_size > 1:
            logger.info("Change delimiter started. local_tmp: %s", local_tmp)
            with open(local_tmp, 'r', encoding=meta_info.ftp_proto_info.encode, errors='replace') as f_in:
                with open(tmp_fullpath, 'w', encoding=meta_info.ftp_proto_info.encode, errors='replace') as f_out:
                    for line in f_in:
                        replace_line = line
                        if meta_info.ftp_proto_info.delimiter == "|^|":
                            replace_line = line.replace("|^|", "\036")
                        if meta_info.ftp_proto_info.delimiter == ",|'":
                            replace_line = line.replace(",|'", "\036")
                        f_out.write(replace_line)
            if os.path.exists(local_tmp):
                os.remove(local_tmp)
                logger.info('local_tmp removed: %s', local_tmp)

            if os.path.exists(tmp_fullpath):
                logger.info("tmp_fullpath exists: %s", tmp_fullpath)
                os.rename(tmp_fullpath, local_tmp)
                logger.info("renamed. tmp_fullpath -> local_tmp. %s -> %s", tmp_fullpath, local_tmp)
        else:
            logger.info("No need to change delimiter")

# cd = ChangeDelimiter()
# cd.change_delimiter("/home/samson/user/wjlee/test_delimiter.txt","|^|")
