import os.path
import re
import zlib
from datetime import datetime
import logging
from dataclasses import dataclass, field
from typing import List
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import copy
import gzip
from collections import namedtuple

from commons_test.MetaInfoHook import InterfaceInfo, SourceInfo
from commons_test.ConvertToParquet_merge import ConvertToParquet
from commons_test.CollectHistory import CollectHistory
from commons_test.KafkaSend import KafkaSend
from commons_test.InfodatManager import InfoDatManager
from commons_test.CompressionManager import CompressionManager



logger = logging.getLogger(__name__)

@dataclass
class ParquetInfo:
    ulid: str = None
    local_full_path: str = None
    local_file_name: str = None
    table_name: str = None
    parquet_hdfs_dir: str = None
    parquet_local_dir: str = None
    parquet_file_name: str = None
    file_index: int = None
    buffer_data: List[str] = field(default_factory=list)

class MergeToParquet:

    def __init__(self, info_dat_path, interface_info: InterfaceInfo, source_info: SourceInfo, collect_history: CollectHistory, kafka_send: KafkaSend):
        self.interface_info = interface_info
        self.source_info = source_info
        self.info_dat_path = info_dat_path

        self.collect_history = collect_history
        self.kafka_send = kafka_send
        self.info_dat_manager = InfoDatManager()

        if self.interface_info.protocol_cd == 'FTP' or self.interface_info.protocol_cd == 'SFTP':
            self.encoding = interface_info.ftp_proto_info.encode
            self.target_time = self.source_info.file_proto_info.target_time
            self.target_compression = self.source_info.file_proto_info.target_compression

        self._parquet_info_init()

    def _file_chunk_reader(self, local_path, ulid, chunk_size):
        file_size = os.path.getsize(local_path)
        _, file_extension = os.path.splitext(local_path)
        self._set_parquet_info(local_path)
        self._send_kafka_end_time(ulid, file_size)
        leftover = ""
        logger.info("put %s to buffer...",local_path)
        with open(local_path, 'r', encoding=self.encoding) as file:
            while True:
                chunk = file.read(chunk_size)
                if not chunk:
                    break
                chunk = leftover + chunk
                lines = chunk.splitlines(keepends=True)
                if not chunk.endswith('\n'):
                    logger.info("putting the left over")
                    leftover = lines.pop()
                else:
                    leftover = ""
                yield "".join(lines)

    def _read_by_buffer(self, buffer_size=1024*1024*1024):

        lines = self._read_infodat()
        self._reset_infodat()

        current_size = 0
        for local_path, dest, ulid in [line.strip().split(",") for line in lines]:
            try:

                if not local_path.endswith(".merge"):
                    self.info_dat_manager.write_info_dat(local_path, os.path.dirname(dest), os.path.basename(local_path), self.info_dat_path, ulid)
                else:
                    local_path = local_path.replace(".merge", "")
                    if os.path.exists(local_path):
                        self._set_parquet_info(local_path)

                        file_size = os.path.getsize(local_path)
                        if file_size <= buffer_size - current_size:
                            self._send_kafka_end_time(ulid, file_size)
                            with open(local_path, 'r', encoding=self.encoding) as file:
                                logger.info("put %s to buffer...", local_path)
                                while True:
                                    chunk = file.read(16 * 1024 * 1024)
                                    if not chunk:
                                        break
                                    self.parquet_info.buffer_data.append(chunk)
                            current_size+=file_size
                        else:
                            logger.info("file size is bigger than left buffer size, file size : %s, left buffer size : %s",file_size, buffer_size - current_size)
                            for chunk in self._file_chunk_reader(local_path, ulid, buffer_size-current_size):
                                self.parquet_info.buffer_data.append(chunk)
                                current_size += len(chunk)

                                if current_size >= buffer_size:
                                    logger.info("buffer full")
                                    logger.info("yield parquet, parquet file name : %s",self.parquet_info.parquet_file_name)
                                    yield copy.deepcopy(self.parquet_info)
                                    self._update_parquet_info()
                                    current_size = 0
            except Exception as e:
                logger.error("Error : %s",e)

        if self.parquet_info.buffer_data:
            logger.info("this is the last buffer")
            logger.info("yield parquet, parquet file name : %s", self.parquet_info.parquet_file_name)
            yield copy.deepcopy(self.parquet_info)

    def start_merge(self):
        logger.info("start merging file")

        self._decompress_gz_file() ##last time logic error occured at here.

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(self._call_convert_to_parquet, parquet_info) for parquet_info in self._read_by_buffer()]

            for future in futures:
                future.add_done_callback(self._call_back)

    def _call_convert_to_parquet(self, parquet_info: ParquetInfo):
        try:
            logger.info("yield parquet file : %s", parquet_info.parquet_file_name)

            logger.info("sending paquet kafka message")
            parquet_local_tmp_path = os.path.join(parquet_info.parquet_local_dir,parquet_info.parquet_file_name+".tmp")
            parquet_local_full_path = os.path.join(parquet_info.parquet_local_dir, parquet_info.parquet_file_name)

            pti = self.partitions.get("partitions")
            ulid = self.collect_history.start_collect_h(self.source_info.collect_source_seq, parquet_local_full_path,
                                                        parquet_info.parquet_hdfs_dir, parquet_info.parquet_file_name,
                                                        self.target_time, pti)
            coll_hist_dict = self.collect_history.chdDict[ulid]
            self.kafka_send.send_to_kafka(coll_hist_dict)

            with open(parquet_local_tmp_path, 'w', encoding=self.encoding) as file:
                file.writelines(parquet_info.buffer_data)

            convert_to_parquet = ConvertToParquet(self.source_info, self.interface_info)
            convert_to_parquet.start_convert(parquet_local_tmp_path, parquet_local_full_path)

            parquet_info.ulid = ulid

            return parquet_info
        except Exception as e:
            logger.error(str(e))
            raise Exception(str(e))

    def _send_kafka_end_time(self, ulid, file_size):
        logger.info("merge file,\n %s -> %s",self.parquet_info.local_full_path, self.parquet_info.parquet_local_dir+"/"+self.parquet_info.parquet_file_name)
        end_time = datetime.now()
        current_time = end_time.isoformat()
        self.collect_history.chdDict[ulid].dest_path = ""
        chd = self.collect_history.chdDict[ulid]
        self.collect_history.set_partition_info(ulid, self.parquet_info.local_file_name, file_size, current_time, self.parquet_info.local_full_path)
        self.collect_history.set_status_code(chd, 4)
        self.kafka_send.send_to_kafka(chd)

    def _extract_table_name(self, hdfs_path):
        match = re.search(r'tb=([^/]+)', hdfs_path)
        if match:
            return match.group(1)
        else:
            logger.warning("there is no table name")
            raise Exception("There is no table name at hdfs path, info dat file : "+ self.info_dat_path)

    def _set_parquet_info(self, local_path):
        self.parquet_info.local_full_path = local_path
        self.parquet_info.local_file_name = os.path.basename(local_path)
        self.parquet_info.parquet_local_dir = os.path.dirname(local_path)

    def _on_completion(self, parquet_info: ParquetInfo):
        logger.info("on_completion started...")
        logger.info("writing to info dat path : %s", self.info_dat_path)
        self._write_to_info_dat(parquet_info)

    def _call_back(self, future):
        try:
            logger.info("call_back started...")
            result = future.result()
            self._on_completion(result)
        except Exception as e:
            logger.error("Error : %s", str(e))
            raise Exception("Failed to write merge file to info date file...")

    def _write_to_info_dat(self, parquet_info: ParquetInfo):
        parquet_local_full_path = os.path.join(parquet_info.parquet_local_dir, parquet_info.parquet_file_name)
        logger.info("parquet_local_path : %s",parquet_local_full_path)
        logger.info("parquet_info.parquet_hdfs_dir : %s", parquet_info.parquet_hdfs_dir)
        logger.info("parquet_info.parquet_file_name : %s", parquet_info.parquet_file_name)
        self.info_dat_manager.write_info_dat(parquet_local_full_path, parquet_info.parquet_hdfs_dir, parquet_info.parquet_file_name, self.info_dat_path, parquet_info.ulid)

    def _open_file(self, local_path):
        if local_path.endswith(".gz"):
            return open(local_path, 'rb')
        else:
            return open(local_path, 'r', encoding=self.encoding)

    def set_partition_info(self, partition):
        self.partitions = partition
        logger.info("hdfs path : %s",self.partitions.get("hdfsPath"))
        self.parquet_info.parquet_hdfs_dir = self.partitions.get("hdfsPath")

    def _parquet_info_init(self):
        self.parquet_info = ParquetInfo(file_index=1)
        logger.info("source_inf.hdfs_dir : %s", self.source_info.hdfs_dir)
        self.parquet_info.table_name = self._extract_table_name(self.source_info.hdfs_dir)

        self.parquet_info.parquet_file_name = (self.parquet_info.table_name + "_" +
                                               self.target_time+"_"+str(self.parquet_info.file_index)
                                               + "." + self.target_compression + ".parquet")
        logger.info("parquet init file name : %s", self.parquet_info.parquet_file_name)

    def _update_parquet_info(self):
        self.parquet_info.file_index += 1
        self.parquet_info.parquet_file_name = (self.parquet_info.table_name + "_" +
                                               self.target_time+"_"+str(self.parquet_info.file_index)
                                               + "." + self.target_compression + ".parquet")
        self.parquet_info.buffer_data = []

    def _decompress_gz_file(self):

        lines = self._read_infodat()
        if self._check_gz_file_in_infodat(lines):
            self._reset_infodat()
            InfoDatInfo = namedtuple('InfoDatInfo', ['local_full_path', 'hdfs_path', 'file_nm', 'info_dat_path', 'ulid'])

            info_dat_infos = []

            compression_manager = CompressionManager()
            for local_path, dest, ulid in [line.strip().split(",") for line in lines]:
                if ".parquet" in local_path:
                    self.info_dat_manager.write_info_dat(local_path, os.path.dirname(dest), os.path.basename(local_path), self.info_dat_path, ulid)
                    continue
                decomp_file_full_path = compression_manager.decompress_gz_with_zlib(local_path)
                decomp_file_nm = os.path.basename(decomp_file_full_path)
                hdfs_path = os.path.dirname(dest)
                if ".merge" in local_path:
                    decomp_file_full_path = decomp_file_full_path + ".merge"
                info_dat_infos.append(InfoDatInfo(local_full_path=decomp_file_full_path, hdfs_path=hdfs_path, file_nm=decomp_file_nm, info_dat_path=self.info_dat_path,ulid=ulid))

            logger.info("rewrite info_dat info with decompressed file path and name..")
            for info_dat_info in info_dat_infos:
                self.info_dat_manager.write_info_dat(info_dat_info.local_full_path, info_dat_info.hdfs_path, info_dat_info.file_nm, info_dat_info.info_dat_path, info_dat_info.ulid)
            logger.info("finished rewriting...")

    def _check_gz_file_in_infodat(self, lines):
        for line in lines:
            if '.gz' in line:
                return True
        return False

    def _read_infodat(self):
        with open(self.info_dat_path, 'r') as f:
            lines = f.readlines()
        return lines

    def _reset_infodat(self):
        with open(self.info_dat_path, 'w') as f:
            pass