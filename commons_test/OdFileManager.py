import ftplib
import logging
import os
import re
from datetime import datetime
from typing import List

from airflow.exceptions import AirflowException
from airflow.models import Variable

from commons_test.FtpSftpDownloader import FtpSftpDownloader
from commons_test.FtpSftpFileHandler import FtpSftpFileHandler
from commons_test.InfodatManager import InfoDatManager
from commons_test.MetaInfoHook import InterfaceInfo, SourceInfo
from commons_test.PathUtil import PathUtil, LocalPathInfos
from commons_test.CollectHistory import CollectHistory
from commons_test.FtpConnector import FtpConnector
from commons_test.SftpConnector import SftpConnector
from commons_test.FtpSftpManager import CollectedFileInfo

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))


class OdFileManager:

    def __init__(self, interface_info: InterfaceInfo, source_infos: List[SourceInfo], kafka_send):
        self.interface_info = interface_info
        self.source_infos = source_infos
        self.kafka_send = kafka_send
        self.infodat = InfoDatManager()
        self.file_downloader = FtpSftpDownloader(kafka_send)
        self.file_handler = FtpSftpFileHandler(kafka_send)
        self.file_count = 0
        self.info_dat_path = None

    def start(self, collect_history, file_path):
        if self.interface_info.protocol_cd == 'SFTP':
            self._start_sftp_get(collect_history, file_path)
        else:
            self._start_ftp_get(collect_history, file_path)

    def _start_ftp_get(self, collect_history, file_path):
        try:
            collected_file_infos = []
            with FtpConnector(self.interface_info.ftp_proto_info, self.interface_info.ftp_proto_info.proxy_setting) as ftp:
                logger.info("Target server source dir : %s", self.interface_info.ftp_proto_info.source_dir)

                filename = os.path.basename(file_path)
                dirname = os.path.dirname(file_path)
                ftp.cwd(dirname)
                for source_info in self.source_infos:
                    local_path_infos, ulid, partitions, coll_hist_dict = self._download_file(ftp, filename, file_path, collect_history, source_info)
                    collected_file_infos.append(CollectedFileInfo(filename, local_path_infos, ulid, partitions, coll_hist_dict, source_info))
                logger.info("%s / %s success ftp get...", self.file_count, len(self.source_infos))
            if self.file_count == 0:
                logger.warning("No file downloaded")
                collect_history.server_info.target_dt = self.interface_info.target_time
                self.kafka_send.sendErrorKafka(collect_history, 5, True, "")
            else:
                for collected_file_info in collected_file_infos:
                    self.file_handler.run(self.interface_info, collect_history, collected_file_info)
        except Exception as e:
            self.kafka_send.sendErrorKafka(collect_history, 8, True, str(e)[:1024])
            raise AirflowException("Error at FTP : " + str(e)[:1024])

    def _start_sftp_get(self, collect_history, file_path):
        try:
            collected_file_infos = []
            with SftpConnector(self.interface_info.ftp_proto_info, self.interface_info.ftp_proto_info.proxy_setting) as sftp:
                    filename = os.path.basename(file_path)
                    for source_info in self.source_infos:
                        local_path_infos, ulid, partitions, coll_hist_dict = self._download_file(sftp, filename, file_path, collect_history, source_info)
                        collected_file_infos.append(CollectedFileInfo(filename, local_path_infos, ulid, partitions, coll_hist_dict, source_info))
                    logger.info("%s / %s success sftp get...", self.file_count, len(self.source_infos))

            if self.file_count == 0:
                logger.warning("No file downloaded")
                collect_history.server_info.target_dt = self.interface_info.target_time
                self.kafka_send.sendErrorKafka(collect_history, 5, True, "")
            else:
                for collected_file_info in collected_file_infos:
                    self.file_handler.run(self.interface_info, collect_history, collected_file_info)
        except Exception as e:
            self.kafka_send.sendErrorKafka(collect_history, 8, True, str(e)[:1024])
            raise AirflowException("Error at FTP : " + str(e)[:1024])

    def _download_file(self, connection, filename, file_path, collect_history: CollectHistory, source_info: SourceInfo):
        logger.info("File target time : %s", source_info.file_proto_info.target_time)
        logger.info("Target server file path : %s", file_path)
        logger.info("Target get file : %s", filename)
        # logger.info("pwd : %s", connection.pwd())
        partitions = PathUtil.get_partition_info(source_info, source_info.file_proto_info.target_time,
                                                 self.interface_info.interface_cycle, filename, self.interface_info.start_time)
        local_path_infos = PathUtil.get_ftp_full_path(self.interface_info, partitions.get("hdfsPath"), filename,
                                                    self.info_dat_path, file_path)
        partition_info = partitions.get("partitions")
        file_target_time = source_info.file_proto_info.target_time
        ulid = collect_history.start_collect_h(source_info.collect_source_seq, local_path_infos.local_tmp_full_path,
                                               partitions.get("hdfsPath"), filename, file_target_time, partition_info)
        collect_history_dict = collect_history.chdDict[ulid]
        self.kafka_send.apply_noti_partition(collect_history_dict, source_info, self.interface_info.target_time)
        self.kafka_send.send_to_kafka(collect_history_dict)
        if self.interface_info.protocol_cd == "FTP":
            self.file_count = self.file_downloader.ftp_download(connection, local_path_infos, filename, collect_history_dict,
                                                                collect_history, file_target_time, self.interface_info, source_info)
        elif self.interface_info.protocol_cd == "SFTP":
            self.file_count = self.file_downloader.sftp_download(connection, local_path_infos, filename, collect_history_dict, collect_history,
                                                                 self.interface_info, source_info)
        self.info_dat_path = local_path_infos.local_info_data_path
        return local_path_infos, ulid, partitions, collect_history_dict

    def get_target_time(self, filename):
        re_1 = r"\d{4,}"
        target_dt = re.search(re_1, filename).group(0)
        return target_dt

    def getInfoDatPath(self):
        if self.info_dat_path is not None:
            return self.info_dat_path
        else:
            return None
