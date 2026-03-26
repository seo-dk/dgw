import ftplib
import logging
from airflow.models import Variable
from typing import Union
import socks
import socket
import subprocess
import psutil
import time

from commons.MetaInfoHook import InterfaceInfo, FtpSftpInterfaceProtoInfo
from commons.ProvideMetaInfoHook import ProvideTargetInfo
from commons.ProxyManager import ProxyManager

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))


class FtpConnector:
    def __init__(self, server_info: Union[FtpSftpInterfaceProtoInfo, ProvideTargetInfo], proxy_setting):
        self.server_info = server_info
        self.proxy_setting = proxy_setting
        self.proxy_manager = ProxyManager(proxy_setting)
        self.ftp_connection = None

    def connect(self):
        logger.info("FTP connection start")
        encode, set_passive = self._init_ftp_resources()
        self.ftp_connection = ftplib.FTP(self.server_info.ip, timeout=60, encoding=encode)
        self.ftp_connection.login(self.server_info.id, self.server_info.pwd)
        self.ftp_connection.set_pasv(set_passive)
        return self.ftp_connection

    def _init_ftp_resources(self):
        encode = "utf-8"
        set_passive = True
        if isinstance(self.server_info, FtpSftpInterfaceProtoInfo):
            encode = self.server_info.encode
            if self.server_info.ftp_trans_mode == "Active":
                set_passive = False
        if self.proxy_setting:
            self.proxy_manager.start_proxy()

            logger.info("set_passive : %s", set_passive)

        return encode, set_passive

    def disconnect(self):
        if self.ftp_connection:
            self.ftp_connection.quit()
            logger.info("FTP connection end")
            self.ftp_connection = None
        self.proxy_manager.stop_proxy()

    def __enter__(self):
        return self.connect()

    def __exit__(self, *args, **kwargs):
        logger.info("positional arguments : %s", args)
        logger.info("keyword arguments : %s", kwargs)
        self.disconnect()