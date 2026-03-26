import fnmatch
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Union
from dataclasses import dataclass
from ftplib import FTP
from paramiko import SFTPClient
import chardet
from airflow.exceptions import AirflowException
from airflow.models import Variable

from commons_test.FtpConnector import FtpConnector
from commons_test.CompressionManager import CompressionManager
from commons_test.InfodatManager import InfoDatManager
from commons_test.MetaInfoHook import InterfaceInfo, SourceInfo
from commons_test.PathUtil import PathUtil
from commons_test.SftpConnector import SftpConnector
from commons_test.PcellJarExecutor import PcellJarExecutor
from commons_test.MergeToParquet import MergeToParquet
from commons_test.CollectHistory import CollectHistory
from commons_test.KafkaSend import KafkaSend
from commons_test.FtpSftpFileHandler import FtpSftpFileHandler
from commons_test.FtpSftpDownloader import FtpSftpDownloader
from commons_test.PathUtil import LocalPathInfos, PartitionInfos

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))


@dataclass
class CollectedFileInfo:
    match_file_nm:str = None
    local_path_infos: LocalPathInfos = None
    ulid: str = None
    partitions: PartitionInfos = None
    coll_hist_dict: Dict[str, str] = None
    source_info: SourceInfo= None



class FtpSftpManager:

    def __init__(self, interface_info: InterfaceInfo, source_infos: List[SourceInfo], kafka_send: KafkaSend):
        self.interface_info = interface_info
        self.source_infos = source_infos
        self.kafka_send = kafka_send
        self.compression_manager = CompressionManager()
        self.infodat = InfoDatManager()
        self.total_file_count = 0

        self.downloader = FtpSftpDownloader(kafka_send)
        self.file_handler = FtpSftpFileHandler(kafka_send)

    def start_collect(self, collect_history):
        if self.interface_info.protocol_cd == "FTP":
            return self._ftp_collect(collect_history)
        elif self.interface_info.protocol_cd == "SFTP":
            return self._sftp_collect(collect_history)

    def _ftp_collect(self, collect_history: CollectHistory):
        try:
            self.info_dat_path = None
            collected_file_infos = []
            with FtpConnector(self.interface_info.ftp_proto_info, self.interface_info.ftp_proto_info.proxy_setting) as ftp:
                self._start_downloading(ftp, collect_history, collected_file_infos)
            self._start_file_handling(collected_file_infos, collect_history)
            return collected_file_infos
        except Exception as e:
            self.kafka_send.sendErrorKafka(collect_history, 8, True, str(e)[:1024])
            raise AirflowException("Error at FTP : " + str(e)[:1024])

    def _sftp_collect(self, collect_history: CollectHistory):
        try:
            if self.interface_info.ftp_proto_info.external_process:
                self.remove_input_files_in_ramdisk()
            self.info_dat_path = None
            collected_file_infos: List[CollectedFileInfo] = []
            with SftpConnector(self.interface_info.ftp_proto_info, self.interface_info.ftp_proto_info.proxy_setting) as sftp:
                self._start_downloading(sftp, collect_history, collected_file_infos)
            self._start_file_handling(collected_file_infos, collect_history)
            return collected_file_infos
        except Exception as e:
            self.kafka_send.sendErrorKafka(collect_history, 8, True, "SFTP connection failure" + str(e)[:1024])
            raise AirflowException("Failed start get via sftp") from e

    def _start_downloading(self, connection: Union[FTP, SFTPClient], collect_history: CollectHistory, collected_file_infos: List[CollectedFileInfo]):
        if self.interface_info.ftp_proto_info.format != 'gz' and self.interface_info.ftp_proto_info.source_file_nm.endswith(".gz"):
            self.check_tar_gz(connection, collect_history, self.source_infos)
        else:
            for source_info in self.source_infos:
                source_dir_original = self.interface_info.ftp_proto_info.source_dir
                if self.interface_info.ftp_proto_info.expand_source_dir_pattern:
                    self._download_from_expanded_dirs(connection, source_info, source_dir_original, collect_history, collected_file_infos)
                else:
                    server_files = self._get_filelist_from_server(connection, source_info)
                    self._collect_match_files_and_download(connection, source_info, server_files, collect_history, collected_file_infos)
                if "*" in source_dir_original or self.interface_info.ftp_proto_info.expand_source_dir_pattern:
                    logger.info("changing back to %s", source_dir_original)
                    self.interface_info.ftp_proto_info.source_dir = source_dir_original
        logger.info("%s / %s success sftp get...", self.total_file_count, len(self.source_infos))

    def _start_file_handling(self, collected_file_infos: List[CollectedFileInfo], collect_history: CollectHistory):
        if self.total_file_count > 0:
            for collected_file_info in collected_file_infos:
                logger.info("start handling %s", collected_file_info)
                self.file_handler.run(self.interface_info, collect_history, collected_file_info)
            for source_info in self.source_infos:
                if self.interface_info.ftp_proto_info.external_process:
                    partitions = PathUtil.get_partition_info(source_info, source_info.file_proto_info.target_time,
                                                             self.interface_info.interface_cycle, None,
                                                             self.interface_info.start_time)
                    executor = PcellJarExecutor(self.interface_info, self.info_dat_path, partitions, self.kafka_send, collect_history)
                    executor.start(source_info)
                self._merge_to_parquet(source_info, collect_history, self.kafka_send, collected_file_infos)
        else:
            logger.info("No file downloaded")
            collect_history.server_info.target_dt = self.interface_info.target_time
            self.kafka_send.sendErrorKafka(collect_history, 5, True, "")

    def remove_input_files_in_ramdisk(self):
        for file in Path(self.interface_info.ftp_proto_info.ramdisk_dir, "in").glob('*'):
            file.unlink()

    def _download_from_expanded_dirs(self, connection: Union[FTP, SFTPClient], source_info: SourceInfo,
                                     source_dir_original: str, collect_history: CollectHistory,
                                     collected_file_infos: List[CollectedFileInfo]):
        """expand_source_dir_pattern 시 패턴 확장된 디렉터리별로 listdir → 매칭 → 다운로드."""
        expanded_dirs = self._expand_source_dir_pattern(connection, source_dir_original)
        logger.info("expand_source_dir_pattern: resolved %s directories", len(expanded_dirs))
        for expanded_dir in expanded_dirs:
            self.interface_info.ftp_proto_info.source_dir = expanded_dir
            try:
                server_files = self._listdir_from_path(connection, expanded_dir)
            except Exception as e:
                logger.warning("listdir failed for %s: %s", expanded_dir, e)
                continue
            path_file_nm_prefix = self._get_wildcard_parts_prefix(source_dir_original, expanded_dir) if self.interface_info.ftp_proto_info.prefix_filenm_with_dir else None
            self._collect_match_files_and_download(connection, source_info, server_files, collect_history, collected_file_infos, path_file_nm_prefix, dir_for_log=expanded_dir)

    def _listdir_from_path(self, connection: Union[FTP, SFTPClient], path: str):
        """지정 경로에서 디렉터리 목록 조회 (FTP/SFTP 분기)."""
        if self.interface_info.protocol_cd == "FTP":
            connection.cwd(path)
            return connection.nlst()
        return connection.listdir(path)

    def _collect_match_files_and_download(self, connection: Union[FTP, SFTPClient], source_info: SourceInfo,
                                         server_files, collect_history: CollectHistory,
                                         collected_file_infos: List[CollectedFileInfo],
                                         path_file_nm_prefix: str = None, dir_for_log: str = None):
        """server_files 기준 패턴 매칭 후 매칭 파일마다 다운로드하여 collected_file_infos에 추가."""
        match_files = self._get_pattern_match_files(server_files, source_info)
        logger.info("match_files %s: %s", ("in " + dir_for_log if dir_for_log else ""), match_files)
        for match_file in match_files:
            local_path_infos, ulid, partitions, coll_hist_dict = self._download_file(
                source_info, match_file, collect_history, connection, path_file_nm_prefix
            )
            collected_file_infos.append(CollectedFileInfo(match_file, local_path_infos, ulid, partitions, coll_hist_dict, source_info))

    def _get_filelist_from_server(self, connection, source_info: SourceInfo):
        source_dir_original = self.interface_info.ftp_proto_info.source_dir

        self._change_wildcard(source_info.file_proto_info.dir_nm)
        ## if ftp get file list by ftp
        server_files = None
        try:
            if self.interface_info.protocol_cd == "FTP":
                connection.cwd(self.interface_info.ftp_proto_info.source_dir)
                server_files = connection.nlst()
            elif self.interface_info.protocol_cd == "SFTP":
                server_files = connection.listdir(self.interface_info.ftp_proto_info.source_dir)
        except Exception as e:
            logger.error("Cannot access to path %s", self.interface_info.ftp_proto_info.source_dir)
            raise Exception(str(e) + ", path: " + self.interface_info.ftp_proto_info.source_dir)

        logger.debug("file count in ftp server: %s", len(server_files))
        logger.debug("test  ftp server: %s", server_files)

        return server_files

    def _get_pattern_match_files(self, server_files, source_info: SourceInfo):
        if not server_files:
            logger.info("No files in target server...")
        match_pattern_files = [file for file in server_files if re.match(source_info.file_proto_info.source_file_nm, file)]
        logger.info("source_file_nm: %s, pattern match count : %s", source_info.file_proto_info.source_file_nm, len(match_pattern_files))

        ## if pattern match result are one and if data_source_filenm is same with match_pattern_files name then enter
        if len(match_pattern_files) == 1 and match_pattern_files[0] == source_info.file_proto_info.source_file_nm:
            return [match_pattern_files[0]]
        else:
            match_name_files = []
            regex_pattern = ""
            if source_info.file_proto_info.use_regex_filename:
                # remove escape
                logger.info("use regex to find file...")
                regex_pattern = source_info.file_proto_info.file_nm.encode().decode('unicode_escape')
                logger.info("regex pattern : %s", regex_pattern)

            for item in match_pattern_files:
                if source_info.file_proto_info.use_regex_filename:
                    if re.match(regex_pattern, item):
                        logger.info("regex_pattern:  %s, item:  %s", regex_pattern, item)
                        match_name_files.append(item)
                else:
                    if fnmatch.fnmatch(item, source_info.file_proto_info.file_nm) or source_info.file_proto_info.file_nm.lower() in item.lower():
                        match_name_files.append(item)

            if len(match_name_files) < 1:
                logger.info("No file match, %s does not exist on the server", source_info.file_proto_info.file_nm)
            logger.info("filename match count : %s", len(match_name_files))

            return match_name_files

    def _get_wildcard_parts_prefix(self, pattern_path: str, expanded_path: str) -> str:
        """패턴 경로에서 * 인 segment에 대응하는 실제 경로 부분만 '_'로 이어 반환. 예: /data/CM/RAN-EMS00*/20260223* 와 /data/CM/RAN-EMS001/2026022301 -> RAN-EMS001_2026022301"""
        pattern_segments = [s for s in pattern_path.split("/") if s]
        expanded_segments = [s for s in expanded_path.rstrip("/").split("/") if s]
        if len(pattern_segments) != len(expanded_segments):
            return os.path.basename(expanded_path.rstrip("/"))
        parts = [expanded_segments[i] for i in range(len(pattern_segments)) if "*" in pattern_segments[i]]
        return "_".join(parts) if parts else os.path.basename(expanded_path.rstrip("/"))

    def _set_path_info(self, source_info: SourceInfo, file_nm, path_file_nm_prefix: str = None):
        if self.interface_info.ftp_proto_info.use_partitions:
            partitions = PathUtil.get_partition_info(source_info, source_info.file_proto_info.target_time, self.interface_info.interface_cycle, file_nm,
                                                     self.interface_info.start_time)
        else:
            partitions = PathUtil.create_non_partitioned_path(source_info)

        local_path_infos = PathUtil.get_ftp_full_path(self.interface_info, partitions.get("hdfsPath"), file_nm, self.info_dat_path)
        if path_file_nm_prefix:
            local_path_infos.local_tmp_full_path = local_path_infos.local_tmp_full_path.parent / (path_file_nm_prefix + "_" + file_nm)

        if source_info.file_proto_info.align_filenm_with_partition:
            local_path_infos = PathUtil.align_filenm_with_partition(file_nm, local_path_infos, partitions)

        return local_path_infos, partitions


    def _download_file(self, source_info: SourceInfo, file_nm, collect_history: CollectHistory, connection, path_file_nm_prefix: str = None):
        """path_file_nm_prefix: 지정 시 경로/저장용 파일명은 prefix_file_nm, 다운로드(RETR/GET)는 원본 file_nm 사용."""
        logger.info("use_partitions : %s", self.interface_info.ftp_proto_info.use_partitions)
        logger.info("file_nm : %s", file_nm)
        download_count = 0
        local_path_infos, partitions = self._set_path_info(source_info, file_nm, path_file_nm_prefix)

        coll_hist_dict, ulid = self._send_kafka_msg(partitions, source_info, collect_history, local_path_infos, file_nm) ## kafka message content return

        if self.interface_info.protocol_cd == "FTP":
            self.file_handler.check_external_process(self.interface_info, source_info, local_path_infos, file_nm)
            download_count += self.downloader.ftp_download(connection, local_path_infos, file_nm, coll_hist_dict, collect_history,
                                                           source_info.file_proto_info.target_time, self.interface_info, source_info)

        elif self.interface_info.protocol_cd == "SFTP":
            self.file_handler.check_external_process(self.interface_info, source_info, local_path_infos, file_nm)
            download_count += self.downloader.sftp_download(connection, local_path_infos, file_nm, coll_hist_dict, collect_history, self.interface_info, source_info)

        if download_count == 0:
            logger.info("%s file download failed", source_info.file_proto_info.file_nm)

        self.info_dat_path = local_path_infos.local_info_data_path
        self.total_file_count += download_count
        return local_path_infos, ulid, partitions, coll_hist_dict

    def check_tar_gz(self, connection, collect_history: CollectHistory, source_infos):
        logger.info("checking if need decompress")
        ftp_files = None
        if self.interface_info.protocol_cd == "FTP":
            connection.cwd(self.interface_info.ftp_proto_info.source_dir)
            ftp_files = connection.nlst()
        elif self.interface_info.protocol_cd == "SFTP":
            ftp_files = connection.listdir(self.interface_info.ftp_proto_info.source_dir)
            ftp_files = [filename.encode(self.interface_info.ftp_proto_info.encode) for filename in ftp_files]

        if self.interface_info.ftp_proto_info.source_dir != self.interface_info.ftp_proto_info.noti_dir:
            raise AirflowException("Cannot get file list from server")

        if not ftp_files:
            raise AirflowException("empty files on ftp server")

        match_noti_files = [notiFileNm for notiFileNm in ftp_files if re.match(self.interface_info.ftp_proto_info.source_file_nm, notiFileNm)]
        gzip_paths = []
        # get gz file at all_files
        local_tmp = None
        local_path_infos = None
        for noti_file in match_noti_files:
            logger.info("interface target_time : %s", self.interface_info.target_time)
            current_time = datetime.now().strftime("%Y%m%d%H")
            gz_path = Path("/test/idcube_out", self.interface_info.interface_id, self.interface_info.target_time[:8])
            local_path_infos = PathUtil.get_ftp_full_path(self.interface_info, gz_path, noti_file, self.info_dat_path)
            local_tmp = local_path_infos.local_tmp_dir
            if self.interface_info.protocol_cd == "FTP":
                gzip_paths.append(
                    self.downloader.ftp_get_tar_gzip(connection, local_path_infos.local_tmp_full_path, noti_file,
                                                     collect_history))
            elif self.interface_info.protocol_cd == "SFTP":
                gzip_paths.append(self.downloader.sftp_get_tar_gzip(connection, local_path_infos, noti_file, collect_history))
            self.info_dat_path = local_path_infos.local_info_data_path

        self.compression_manager.check_file_in_targz(gzip_paths, local_tmp, source_infos)
        self.total_file_count = self.write_gz_file(os.listdir(local_tmp), collect_history, local_path_infos)

    def write_gz_file(self, local_file_list, collect_history: CollectHistory, local_path_infos: LocalPathInfos):
        file_count = 0
        if local_file_list is not None:
            for source_info in self.source_infos:
                match_name_files_gz = [item for item in local_file_list if source_info.file_proto_info.file_nm in item]
                for match_nm_file in match_name_files_gz:
                    fileFullPath = Path(local_path_infos.local_tmp_dir, match_nm_file)
                    _, partitions = self._set_path_info(source_info, match_nm_file)
                    collect_hist_dict, ulid = self._send_kafka_msg(partitions,source_info, collect_history,local_path_infos, match_nm_file)
                    ## write on info.dat file witch matches
                    self.infodat.write_info_dat(fileFullPath, partitions.get("hdfsPath"), match_nm_file, local_path_infos.local_info_data_path, ulid)
                    self.kafka_send.send_to_kafka(collect_hist_dict)
                    file_count = file_count + 1
        logger.info("gz matched file list count : %s", file_count)
        return file_count

    def _send_kafka_msg(self, partitions, source_info: SourceInfo, collect_history: CollectHistory, local_path_infos: LocalPathInfos, file_nm):
        pti = partitions.get("partitions")
        ulid = collect_history.start_collect_h(source_info.collect_source_seq, local_path_infos.local_tmp_full_path,
                                               partitions.get("hdfsPath"), file_nm, source_info.file_proto_info.target_time, pti)
        coll_hist_dict = collect_history.chdDict[ulid]
        self.kafka_send.send_to_kafka(coll_hist_dict)
        return coll_hist_dict, ulid

    def get_info_dat_path(self):
        if self.info_dat_path is not None:
            return self.info_dat_path
        else:
            return None

    def get_encoded_string(self, data):
        detected_encode = chardet.detect(data)
        encoding = detected_encode['encoding']
        logger.info("encoding : %s", encoding)
        encode_dict = {'encoding': encoding, 'decode_str': data.decode(encoding, 'ignore')}
        return encode_dict

    def _expand_source_dir_pattern(self, connection: Union[FTP, SFTPClient], source_dir: str) -> List[str]:
        """source_dir 패턴(와일드카드 포함)을 서버에서 listdir + fnmatch로 확장해 실제 경로 목록 반환."""
        segments = [s for s in source_dir.split("/") if s]
        if not segments:
            return [source_dir.rstrip("/") or "/"]

        def expand_recursive(current_path: str, segment_index: int) -> List[str]:
            if segment_index >= len(segments):
                return [current_path]
            seg = segments[segment_index]
            next_path = (current_path.rstrip("/") + "/" + seg) if current_path else ("/" + seg)

            if "*" not in seg:
                return expand_recursive(next_path, segment_index + 1)

            try:
                if self.interface_info.protocol_cd == "FTP":
                    connection.cwd(current_path or "/")
                    entries = connection.nlst()
                else:
                    entries = connection.listdir(current_path or "/")
            except Exception as e:
                logger.warning("listdir failed for %s: %s", current_path, e)
                return []

            matched = [e for e in entries if fnmatch.fnmatch(e, seg)]
            results = []
            for entry in matched:
                entry_path = (current_path.rstrip("/") + "/" + entry) if current_path else ("/" + entry)
                try:
                    results.extend(expand_recursive(entry_path, segment_index + 1))
                except Exception as e:
                    logger.debug("skip non-dir or listdir fail %s: %s", entry_path, e)
            return results

        return expand_recursive("", 0)

    def _change_wildcard(self, dirnm):
        if dirnm:
            source_dir_arr = self.interface_info.ftp_proto_info.source_dir.split("/")
            index = source_dir_arr.index("*")
            logger.info("Found wild card in source_dir, index : %s", index)
            source_dir_arr[index] = dirnm
            logger.info("source_dir change, before : %s", self.interface_info.ftp_proto_info.source_dir)
            self.interface_info.ftp_proto_info.source_dir = '/'.join(source_dir_arr)
            logger.info("source_dir change after : %s", self.interface_info.ftp_proto_info.source_dir)

    def _merge_to_parquet(self, source_info: SourceInfo, collect_history: CollectHistory, kafka_send: KafkaSend, collected_file_infos: List[CollectedFileInfo]):

        try:
            if source_info.file_proto_info.merge and source_info.file_proto_info.convert_to:
                if any(collected_file_info.match_file_nm.endswith("tar") for collected_file_info in collected_file_infos):
                    logger.info(".tar file has been already merged")
                    return
                logger.info("start merge file to parquet...")
                merge_to_parquet = MergeToParquet(self.info_dat_path, self.interface_info, source_info, collect_history, kafka_send)
                merge_parquet_file_name = merge_to_parquet.parquet_info.parquet_file_name
                partitions = PathUtil.get_partition_info(source_info, source_info.file_proto_info.target_time, self.interface_info.interface_cycle, merge_parquet_file_name,
                                                         self.interface_info.start_time)
                merge_to_parquet.set_partition_info(partitions)
                merge_to_parquet.start_merge()
                logger.info("merge %s -> %s...", self.total_file_count, merge_to_parquet.parquet_info.file_index)
        except Exception as e:
            logger.error("error during merge : %s", e)
            raise Exception(e)
