import logging
import os
from airflow.exceptions import AirflowException
from airflow.models import Variable

from commons_test.CompressionManager import CompressionManager

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))

def remove_header(local_full_path, interface_info):
    logger.info("local_full_path : %s", local_full_path)

    encode = "UTF-8"

    if interface_info.protocol_cd == 'FTP' or interface_info.protocol_cd == 'SFTP':
        encode = interface_info.ftp_proto_info.encode
        if interface_info.ftp_proto_info.format == 'gz':
            compression_manager = CompressionManager()
            local_full_path = compression_manager.decompress_gz_file(local_full_path)
    elif interface_info.protocol_cd == 'DISTCP':
        encode = interface_info.distcp_proto_info.encode

    tmp_file_path = str(local_full_path) + ".tmp"

    with open(local_full_path, 'r', encoding=encode) as local_file:
        next(local_file)
        with open(tmp_file_path, 'w', encoding=encode) as tmp_file:
            for line in local_file:
                tmp_file.write(line)
    if os.path.exists(tmp_file_path):
        os.rename(tmp_file_path, local_full_path)
    logger.info("header removed successfully..")

