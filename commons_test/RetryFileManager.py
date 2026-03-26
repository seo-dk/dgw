import copy
import logging
import os
import re
import socket
from pathlib import Path
from typing import List

from airflow.exceptions import AirflowException
from airflow.models import Variable

from commons_test.FtpConnector import FtpConnector
from commons_test.FtpHandler import FtpHandler
from commons_test.CompressionManager import CompressionManager
from commons_test.InfodatManager import InfoDatManager
from commons_test.Util import Util
from commons_test.SftpConnector import SftpConnector
from commons_test.PathUtil import PathUtil, LocalPathInfos
from commons_test.CollectHistory import CollectHistory
from commons_test.KafkaSend import KafkaSend
from commons_test.MetaInfoHook import InterfaceInfo, SourceInfo

from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))

##d아직 리팩토링 이전. commons_test에 올리면 안됨.

class RetryFileManager:

    def __init__(self, meta_info: InterfaceInfo, file_list_info: List[SourceInfo], kafka_send: KafkaSend):
        self.meta_info = meta_info
        self.file_list_info = file_list_info
        self.kafka_send = kafka_send
        self.file_count = 0
        self.ftp_get = FtpHandler(kafka_send)
        self.compression_manager = CompressionManager()
        self.infodat = InfoDatManager()
        self.connected = False
        self.ftp_connector = FtpConnector(self.meta_info.ftp_proto_info, self.meta_info.ftp_proto_info.proxy_setting)
        self.sftp_connector = SftpConnector(self.meta_info.ftp_proto_info, self.meta_info.ftp_proto_info.proxy_setting)

    def ftp_retry_get(self, collect_history: CollectHistory, files):
        retry_id_seq = None
        ftp = None
        self.info_dat_path = None
        try:
            for line in files:
                collect_history = self._set_kafka_message(line,collect_history,True)
                self.kafka_send.send_to_kafka(collect_history.server_info)
                retry_info = line.split(",")
                retry_target_dt = retry_info[3]
                collect_hist_id = retry_info[1]
                retry_id_seq = int(retry_info[6])
                if not self.connected:
                    ftp = self.ftp_connector.connect()
                    self.connected = True
                if self.connected:
                    logger.info("Retry list length : %s", len(files))
                    logger.info("file count : %s", self.file_count)
                    logger.info("Restart target dt : %s", retry_target_dt)

                    interface_info = copy.deepcopy(self.meta_info)
                    ## file info that matches with retry info
                    file_list_info = copy.deepcopy(self.file_list_info)

                    interface_info.interface_cycle = self.set_interface_cycle(retry_target_dt, interface_info)
                    ## change targetdt with retry targetdt
                    Util.replace_time_pattern(file_list_info, interface_info, retry_target_dt)
                    logger.info("interface_info : %s", interface_info)

                    if self.meta_info.ftp_proto_info.format != 'gz' and self.meta_info.ftp_proto_info.source_file_nm.endswith(
                            ".gz"):
                        self._check_noti(ftp, collect_history, retry_info, file_list_info, interface_info)
                    else:
                        self.find_match_file(file_list_info, collect_history, retry_info, ftp, interface_info)

                    if self.file_count == 0:
                        logger.warn("No file downloaded")
                        collect_history.server_info.collect_hist_id = collect_hist_id
                        collect_history.server_info.retry = False
                        collect_history.server_info.retry_id_seq = retry_id_seq
                        collect_history.server_info.target_dt = self.meta_info.target_time
                        self.kafka_send.sendErrorKafka(collect_history, 5, False, "")
            if ftp:
                logger.info("Quit ftp connection..")
                self.ftp_connector.disconnect()
        except socket.timeout as e:
            logger.info("FTP Timeout!!")
            for line in files:
                collect_history = self._set_kafka_message(line,collect_history,False)
                self.kafka_send.sendErrorKafka(collect_history, 8, False, str(e)[:1024])
            raise AirflowException("Error at FTP : " + str(e)[:1024])
        except Exception as e:
            logger.exception("FTP failed...")
            for line in files:
                collect_history = self._set_kafka_message(line,collect_history,False)
                self.kafka_send.sendErrorKafka(collect_history, 8, False, str(e)[:1024])
            if ftp:
                logger.info("Quit ftp connection..")
                self.ftp_connector.disconnect()
            raise AirflowException("Error at FTP : " + str(e)[:1024])

    def sftp_retry_get(self, collect_history: CollectHistory, files):
        self.info_dat_path = None
        retry_id_seq = None
        sftp = None
        ssh_client = None
        try:
            for line in files:
                collect_history = self._set_kafka_message(line, collect_history, True)
                self.kafka_send.send_to_kafka(collect_history.server_info)
                retry_info = line.split(",")
                collect_hist_id = retry_info[1]
                retry_target_dt = retry_info[3]
                retry_id_seq = int(retry_info[6])
                if not self.connected:
                    sftp = self.sftp_connector.sftp_connect()
                    self.connected = True
                if self.connected:
                    logger.info("Restart target dt : %s", retry_target_dt)

                    interface_info = copy.deepcopy(self.meta_info)
                    file_list_info = copy.deepcopy(self.file_list_info)
                    interface_info.interface_cycle = self.set_interface_cycle(retry_target_dt, interface_info)

                    Util.replace_time_pattern(file_list_info, interface_info, retry_target_dt)

                    if interface_info.ftp_proto_info.format != 'gz' and interface_info.ftp_proto_info.source_file_nm.endswith(
                            ".gz"):
                        self._check_noti(sftp, collect_history, retry_info, file_list_info, interface_info)
                    else:
                        self.find_match_file(file_list_info, collect_history, retry_info, sftp, interface_info)

                    if self.file_count == 0:
                        logger.warn("No file downloaded")
                        collect_history.server_info.collect_hist_id = collect_hist_id
                        collect_history.server_info.retry = False
                        collect_history.server_info.target_dt = self.meta_info.target_time
                        collect_history.server_info.retry_id_seq = retry_id_seq
                        self.kafka_send.sendErrorKafka(collect_history, 5, False, "")
            if sftp:
                logger.info("Quit sftp connection..")
                self.sftp_connector.sftp_disconnect()
        except socket.timeout as e:
            logger.info("SFTP Timeout!!")
            for line in files:
                collect_history = self._set_kafka_message(line,collect_history,False)
                self.kafka_send.sendErrorKafka(collect_history, 8, False, str(e)[:1024])
            if sftp:
                logger.info("Quit sftp connection..")
                self.sftp_connector.sftp_disconnect()
            raise AirflowException("Error at FTP : " + str(e)[:1024])
        except Exception as e:
            logger.exception("SFTP failed...")
            for line in files:
                collect_history = self._set_kafka_message(line,collect_history,False)
                self.kafka_send.sendErrorKafka(collect_history, 8, False, str(e)[:1024])
            if sftp:
                logger.info("Quit sftp connection..")
                self.sftp_connector.sftp_disconnect()
            raise AirflowException("Error at FTP : " + str(e)[:1024])

        ## all_files : file list of target server, file_list_info : file info that matches with retry info, retry_info : retry file info

    def find_match_file(self, file_list_info: List[SourceInfo], collect_history: CollectHistory, retry_info, connection, interface_info):
        flag = False
        for file_info in file_list_info:
            # before change source_dir get source_dir for next loop
            # check wild card text in data_source_dir

            source_dir_original = interface_info.ftp_proto_info.source_dir

            self._change_wildcard(file_info.file_proto_info.dir_nm, interface_info)
            ## if ftp get file list by ftp
            if self.meta_info.protocol_cd == "FTP":
                connection.cwd(interface_info.ftp_proto_info.source_dir)
                all_files = connection.nlst()
            elif self.meta_info.protocol_cd == "SFTP":
                all_files = connection.listdir(interface_info.ftp_proto_info.source_dir)

            logger.info("file list length : %s", len(all_files))

            ## find a file that matches with DATA_SOURCE_FILE_NM in target server file list
            match_pattern_files = [dsfnm for dsfnm in all_files if
                                   re.match(file_info.file_proto_info.source_file_nm, dsfnm)]

            if len(match_pattern_files) == 1 and match_pattern_files[0] == file_info.file_proto_info.source_file_nm:
                flag = self.check_retry_download(retry_info, file_info, match_pattern_files[0], collect_history,
                                                 connection, interface_info)
            else:
                ## find a file that matches with FILE_NM in match_pattern_files
                match_name_files = [item for item in match_pattern_files if file_info.file_proto_info.file_nm in item]
                for match_name in match_name_files:
                    ## if retry has no file name, it should download same target_dt file
                    logger.info("Match file : %s", match_name)
                    flag = self.check_retry_download(retry_info, file_info, match_name, collect_history, connection,
                                                     interface_info)
                    ## no need to search other file
                    if flag:
                        break
            if flag:
                break
            if "*" in source_dir_original:
                logger.info("changing back to %s", source_dir_original)
                interface_info.ftp_proto_info.source_dir = source_dir_original
        logger.info("FTP get %s / %s", self.file_count, len(file_list_info))

    def check_retry_download(self, retry_info, file_info: SourceInfo, filenm, collect_history: CollectHistory, connection, interface_info,
                             local_path_infos: LocalPathInfos=None, noti=False):
        ## when filenm not exist
        if retry_info[7] == "":
            logger.info("file_name does not exist")
            self.set_path_get(file_info, filenm, collect_history, connection, interface_info, noti, local_path_infos)
            return False
        ## when filenm exist
        if retry_info[7] == filenm:
            logger.info("file_name does exist")
            self.set_path_get(file_info, filenm, collect_history, connection, interface_info, noti, local_path_infos,
                              retry_info)
            return True
        if filenm.endswith(".tar"):
            logger.info("file extenstion is tar")
            self.set_path_get(file_info, filenm, collect_history, connection, interface_info, noti, local_path_infos,
                              retry_info)
            return True
        if retry_info[7] != filenm:
            logger.info("file name is not equal with retry file...")
            return False

            ##collect_hist_id=None,started_at=None,retry_id_seq=None

    ##"C-MFAS-FTP-HH-0001,01H6B2MKSPP44XVNVAY2MKWKR4,3841,202307271500,FTP,2023-07-27T16:05:02.077387,2,WCDMA_F3000_FAULT_MSG_20230727_1535.dat"

    ## file_info : download file_info, filenm : download filenm
    def set_path_get(self, file_info: SourceInfo, filenm, collect_history: CollectHistory, connection, interface_info, noti, local_path_infos: LocalPathInfos=None,
                     retry_info=None):
        target_time = file_info.file_proto_info.target_time

        if not self.meta_info.ftp_proto_info.use_partitions:
            partitions = PathUtil.create_non_partitioned_path(file_info)
        else:
            partitions = PathUtil.get_partition_info(file_info, target_time,
                                                     interface_info.interface_cycle, filenm, interface_info.start_time)

        pti = partitions.get("partitions")
        ulid = None

        current_time = datetime.now().isoformat()

        if noti:
            if retry_info:
                ulid = collect_history.start_collect_h(file_info.collect_source_seq, local_path_infos.local_tmp_full_path,
                                                      partitions.get("hdfsPath"),
                                                      filenm, target_time, pti, retry_info[1], current_time,
                                                      int(retry_info[6]))
            else:
                ulid = collect_history.start_collect_h(file_info.collect_source_seq, local_path_infos.local_tmp_full_path,
                                                      partitions.get("hdfsPath"),
                                                      filenm, target_time, pti)
            self.infodat.write_info_dat(local_path_infos.local_tmp_full_path, partitions.get("hdfsPath"), filenm,
                                        local_path_infos.local_info_data_path, ulid)
            chd = collect_history.chdDict[ulid]
            self.kafka_send.send_to_kafka(chd)
            self.info_dat_path = local_path_infos.local_info_data_path
            self.file_count = self.file_count + 1
        else:
            local_path_infos = PathUtil.get_ftp_full_path(interface_info, file_info, partitions.get("hdfsPath"), filenm,
                                                      self.info_dat_path)
            if retry_info:
                ulid = collect_history.start_collect_h(file_info.collect_source_seq, local_path_infos.local_tmp_full_path,
                                                      partitions.get("hdfsPath"),
                                                      filenm, target_time, pti, retry_info[1], current_time,
                                                      int(retry_info[6]))
            else:
                ulid = collect_history.start_collect_h(file_info.collect_source_seq, local_path_infos.local_tmp_full_path,
                                                      partitions.get("hdfsPath"),
                                                      filenm, target_time, pti)
            chd = collect_history.chdDict[ulid]
            self.kafka_send.send_to_kafka(chd)
            if self.meta_info.protocol_cd == "FTP":
                self.file_count = self.ftp_get.ftp_download(connection, local_path_infos, filenm, chd, collect_history,
                                                            self.file_count, partitions,
                                                            ulid, target_time, interface_info, file_info)
            elif self.meta_info.protocol_cd == "SFTP":
                self.file_count = self.ftp_get.sftp_download(connection, local_path_infos, filenm, chd, collect_history,
                                                             self.file_count, partitions,
                                                             ulid, target_time, interface_info, file_info)
            self.info_dat_path = local_path_infos.local_info_data_path

    def _check_noti(self, connection, collect_history: CollectHistory, retry_info, file_list_info, interface_info):

        if self.meta_info.protocol_cd == "FTP":
            connection.cwd(interface_info.ftp_proto_info.source_dir)
            all_files = connection.nlst()
        elif self.meta_info.protocol_cd == "SFTP":
            all_files = connection.listdir(interface_info.ftp_proto_info.source_dir)
            all_files = [filename.encode(interface_info.ftp_proto_info.encode) for filename in all_files]

        logger.info("check gzip file...")
        # get gz file at all_files
        match_noti_files = [noti_file_nm for noti_file_nm in all_files if
                            re.match(interface_info.ftp_proto_info.source_file_nm, noti_file_nm)]
        gzip_paths = []
        logger.info("match gzip files : %s", len(match_noti_files))
        logger.info("server info data source filenm : %s", interface_info.ftp_proto_info.source_file_nm)
        # get gz file at all_files
        local_tmp = None
        for noti_file in match_noti_files:
            gz_path = Path("/idcube_out", self.meta_info.interface_id, interface_info.target_time[:8])
            local_path_infos = PathUtil.get_ftp_full_path(self.meta_info, gz_path, noti_file, self.info_dat_path)
            local_tmp = local_path_infos.local_tmp_dir
            # download gzip file
            if self.meta_info.protocol_cd == "FTP":
                gzip_paths.append(self.ftp_get.ftp_get_tar_gzip(connection, local_tmp, noti_file, collect_history))
            elif self.meta_info.protocol_cd == "SFTP":
                gzip_paths.append(self.ftp_get.sftp_get_tar_gzip(connection, local_tmp, noti_file, collect_history))
            # To check if the filenm exist in gzip file
            self.info_dat_path = local_path_infos.local_info_data_path
        self.compression_manager.check_file_in_targz(gzip_paths, local_tmp, file_list_info)

        # get file list from local_tmp
        decomp_file_list = os.listdir(local_tmp)
        # write the files in the gzip file to info.dat
        self._write_gz_file(decomp_file_list, collect_history, local_path_infos, retry_info, file_list_info, connection,
                            interface_info)
        self.file_count = 0

    def _write_gz_file(self, decomp_file_list, collect_history: CollectHistory, local_path_infos: LocalPathInfos, retry_info, file_list_info, connection,
                       interface_info):
        flag = False
        #if decompress success enter
        if decomp_file_list is not None:
            # find the match file name with collect_source4 filenm
            for file_info in file_list_info:
                ## list that match with filenm in decompress file
                match_name_files_gz = [item for item in decomp_file_list if file_info.file_proto_info.file_nm in item]
                for match_nm_file in match_name_files_gz:

                    flag = self.check_retry_download(retry_info, file_info, match_nm_file, collect_history, connection,
                                                     interface_info, local_path_infos, noti=True)
                    ## if it has file name break the loop
                    if flag:
                        break
                ## if it has file name break the loop
                if flag:
                    break

    def getInfoDatPath(self):
        if self.info_dat_path is not None:
            return self.info_dat_path
        else:
            return None

    def set_interface_cycle(self, retry_target_dt, interface_info):
        if interface_info.interface_cycle == "OD":
            if len(retry_target_dt) == 6:
                return "MM"
            elif len(retry_target_dt) == 8:
                return "DD"
            elif len(retry_target_dt) >= 10:
                return "HH"
        else:
            return interface_info.interface_cycle

    def _change_wildcard(self, dir_nm, interface_info):
        if dir_nm:
            source_dir_arr = interface_info.ftp_proto_info.source_dir.split("/")
            index = source_dir_arr.index("*")
            logger.info("Found wild card in source_dir, index : %s", index)
            source_dir_arr[index] = dir_nm
            logger.info("source_dir change, before : %s", interface_info.ftp_proto_info.source_dir)
            interface_info.ftp_proto_info.source_dir = '/'.join(source_dir_arr)
            logger.info("source_dir change after : %s", interface_info.ftp_proto_info.source_dir)

    def _set_kafka_message(self, line, collect_history: CollectHistory, retry_process):
        retry_info = line.split(",")
        collect_history.server_info.collect_hist_id = retry_info[1]
        collect_history.server_info.retry = retry_process
        collect_history.server_info.retry_id_seq = int(retry_info[6])
        return collect_history