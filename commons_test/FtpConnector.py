import ftplib
import logging
from airflow.models import Variable
from typing import Union
import socks
import socket
import subprocess
import psutil
import time

from commons_test.MetaInfoHook import InterfaceInfo, FtpSftpInterfaceProtoInfo
from commons_test.ProvideMetaInfoHook import ProvideTargetInfo
from commons_test.ProxyManager import ProxyManager

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))


class FtpConnector:
    def __init__(self, server_info: Union[FtpSftpInterfaceProtoInfo, ProvideTargetInfo], proxy_setting):
        self.server_info = server_info
        self.proxy_setting = proxy_setting
        self.proxy_manager = ProxyManager(proxy_setting)
        self.ftp_connection = None
        self.binding_ip = None
        if isinstance(self.server_info, FtpSftpInterfaceProtoInfo) and self.server_info.binding_ip:
            self.binding_ip = self.server_info.binding_ip

    def connect(self):
        logger.info("FTP connection start")
        encode, set_passive = self._init_ftp_resources()
        
        # 목적지 FTP 서버 포트
        if hasattr(self.server_info, "port") and self.server_info.port:
            port = int(self.server_info.port)
        else:
            logger.error("FTP port is not set in server_info")
            raise ValueError("FTP port is required")
        logger.info(f"FTP target: {self.server_info.ip}:{port}")
        
        # binding_ip가 설정되어 있으면 소켓 바인딩하여 연결 (nc -s와 동일)
        source_address = None
        if self.binding_ip:
            logger.info(f"Using binding IP: {self.binding_ip} (same as 'nc -s {self.binding_ip}')")
            source_address = (self.binding_ip, 0)  # 0은 시스템이 자동으로 사용 가능한 포트 할당
        
        self.ftp_connection = ftplib.FTP(
            timeout=60,
            encoding=encode,
            source_address=source_address
        )
        self.ftp_connection.connect(self.server_info.ip, port, timeout=60)
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

            if self.proxy_manager.get_ssh_process():
                set_passive = True

            logger.info(f"set_passive : {set_passive}")

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
        logger.info(f"positional arguments : {args}")
        logger.info(f"keyword arguments : {kwargs}")
        self.disconnect()
