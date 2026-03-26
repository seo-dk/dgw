import os
import paramiko
import logging
from typing import Union

from airflow.models import Variable

from commons.MetaInfoHook import InterfaceInfo, FtpSftpInterfaceProtoInfo
from commons.ProvideMetaInfoHook import ProvideTargetInfo
from commons.ProxyManager import ProxyManager

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))


class SftpConnector:
    def __init__(self, interface_info: Union[FtpSftpInterfaceProtoInfo, ProvideTargetInfo], proxy_setting):
        self.interface_info = interface_info
        self.sftp_connection = None
        self.ssh_connection = None
        self.proxy_setting = proxy_setting
        self.proxy_manager = ProxyManager(proxy_setting)

    def _connect(self, use_ssh_key=False):
        self.ssh_connection.connect(
            hostname=self.interface_info.ip,
            port=self.interface_info.port,
            username=self.interface_info.id,
            pkey=paramiko.RSAKey.from_private_key_file(os.path.expanduser("~/.ssh/id_rsa")) if use_ssh_key else None,
            password=None if use_ssh_key else self.interface_info.pwd,
            timeout=30
        )

    def sftp_connect(self):
        self.ssh_connection = paramiko.SSHClient()
        self.ssh_connection.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        logger.info("Start getting files from source server... meta_info: %s", self.interface_info)
        self._connect(use_ssh_key=self.interface_info.use_ssh_key)
        self.sftp_connection = self.ssh_connection.open_sftp()
        return self.sftp_connection

    def sftp_disconnect(self):
        if self.ssh_connection and self.sftp_connection:
            logger.info("closing sftp connection")
            self.sftp_connection.close()
            self.ssh_connection.close()
            logger.info("connection closed")

    def __enter__(self):
        if self.proxy_setting:
            self.proxy_manager.start_proxy()
        return self.sftp_connect()

    def __exit__(self, *args, **kwargs):
        logger.info("exit ...")
        self.sftp_disconnect()
        self.proxy_manager.stop_proxy()


if __name__ == "__main__":
    # class FakeInterfaceInfo(InterfaceInfo):

    interface_info = InterfaceInfo()
    interface_info.ftp_proto_info = FtpSftpInterfaceProtoInfo()
    interface_info.ftp_proto_info.ip = '90.90.31.110'
    interface_info.ftp_proto_info.port = 30022
    interface_info.ftp_proto_info.id = 'sktkisa'
    interface_info.ftp_proto_info.pwd = ''

    with SftpConnector(interface_info.ftp_proto_info, None):
        print("SFTP connection established.")

    print("SFTP connection closed.")


    interface_info2 = InterfaceInfo()
    interface_info2.ftp_proto_info = FtpSftpInterfaceProtoInfo()
    interface_info2.ftp_proto_info.ip = '90.90.90.52'
    interface_info2.ftp_proto_info.port = 22
    interface_info2.ftp_proto_info.id = 'ftpuser_datagw'
    interface_info2.ftp_proto_info.pwd = 'shfkd2023^_'

    with SftpConnector(interface_info.ftp_proto_info, None):
        print("SFTP connection established.")

    print("SFTP connection closed.")
