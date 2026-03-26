import logging
import os
import re
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass
from typing import Dict

import ulid
from airflow.models import Variable

from commons.MetaInfoHook import InterfaceInfo, SourceInfo
from commons.Util import Util

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))

@dataclass
class PartitionInfos:
    partitions: str = None
    hdfs_path: str = None


@dataclass
class LocalPathInfos:
    source_file_path: str = None
    local_tmp_dir: str = None
    local_tmp_full_path: str = None
    local_info_data_path: str = None


class PathUtil:

    @classmethod
    def get_ftp_full_path(cls, interface_info: InterfaceInfo, file_path, match_file_nm, info_dat_path, os_file_path=None):

        local_path = LocalPathInfos()

        logger.info("Matched file name : %s", match_file_nm)
        if os_file_path:
            source_file_path = os_file_path
        else:
            source_file_path = Path(str(interface_info.ftp_proto_info.source_dir), match_file_nm)
            logger.info("Source file path : %s", source_file_path)

        if not interface_info.ftp_proto_info.use_partitions:
            filename = cls.remove_date_filenm(match_file_nm)
        else:
            filename = os.path.basename(source_file_path)

        local_path.source_file_path = source_file_path

        tmp_path = "/data" + str(file_path)

        logger.info("tmp_path : %s", tmp_path)
        os.makedirs(tmp_path, exist_ok=True)

        local_path.local_tmp_dir = tmp_path

        if interface_info.ftp_proto_info.external_process:
            local_path.local_tmp_full_path = Path(interface_info.ftp_proto_info.ramdisk_dir, "in", filename)
        else:
            local_path.local_tmp_full_path = Path(tmp_path, filename)

        logger.info("local tmp full path : %s", local_path.local_tmp_full_path)
        new_ulid = ulid.new()

        if info_dat_path is not None:
            local_path.local_info_data_path = info_dat_path
        else:
            local_path.local_info_data_path = Path("/data/gw_meta", interface_info.interface_id, "info_" + str(new_ulid) + ".dat")

        return local_path

    @classmethod
    def _has_group_capture(cls, regex):
        pattern = re.compile(r'\([^)]*\)')

        # 괄호가 있는지 확인
        if pattern.search(regex):
            return True
        else:
            return False

    @classmethod
    def get_partition_info(cls, source_info: SourceInfo, target_time, interface_cycle, filename, started_time):

        partDict = {}
        dictArr = []

        if source_info.file_proto_info:
            if source_info.file_proto_info.partitions:
                dictArr.extend(cls._process_partitions(source_info, target_time, interface_cycle, filename, started_time))
            else:
                dictArr.extend(cls._get_dt_path(target_time, interface_cycle))
        else:
            dictArr.extend(cls._get_dt_path(target_time, interface_cycle, True))

        hdfsPath = cls.get_hdfs_path(dictArr, source_info)

        partDict["partitions"] = dictArr
        partDict["hdfsPath"] = hdfsPath

        return partDict

    @classmethod
    def get_distcp_batch_partition_segments(cls, source_info: SourceInfo, interface_info: 'InterfaceInfo', formatted_start_time, filename: str = ""):
        """DistCP batch 수집소 proto의 partitions를 FTP/SFTP와 동일 규칙으로 처리한다 (${yyyyMMdd}, ${yyyyMMdd-1d}, dt/hh/hm, 비정규식 리터럴 등)."""
        if not source_info.distcp_proto_info or not source_info.distcp_proto_info.partitions:
            return None
        return cls._process_partitions(
            source_info,
            interface_info.target_time,
            interface_info.interface_cycle,
            filename,
            formatted_start_time,
        )

    @classmethod
    def _process_partitions(cls, file_info, target_time, interface_cycle, filename, started_time):
        dict_arr = []
        dt_exist = False
        partitions = None

        date_keys = ["dt", "hh", "hm"]

        if file_info.distcp_proto_info:
            partitions = file_info.distcp_proto_info.partitions
        if file_info.kafka_proto_info:
            partitions = file_info.kafka_proto_info.partitions
        if file_info.file_proto_info:
            partitions = file_info.file_proto_info.partitions

        if not partitions:
            return []

        for partition in partitions:
            logger.info("dict : %s", partition)
            key = list(partition.keys())[0]
            if key in date_keys:
                dict_arr.extend(cls._process_dt(partition, key, interface_cycle, started_time))
                dt_exist = True
            else:
                dict_arr.extend(cls._process_non_dt(partition, filename))

        if not dt_exist:
            dict_arr = cls._get_dt_path(target_time, interface_cycle) + dict_arr

        return dict_arr

    @classmethod
    def _process_dt(cls, partition: Dict[str, str], key, interface_cycle, started_time):
        logger.info("started_time : %s", started_time)
        partition_arr = []
        time_pattern = partition[key]
        logger.info("partition time_pattern : %s",time_pattern)
        if Util.get_time_pattern(time_pattern):
            formatted_time = Util.get_calculate_pattern(time_pattern, started_time)
        else:
            formatted_time = partition[key]
        logger.info("partition dt time : %s", formatted_time)

        if key == "dt":
            partition[key] = formatted_time[:8]
        partition[key] = formatted_time
        partition_arr.append(partition)

        if len(formatted_time) > 8:
            new_dict = {}
            if interface_cycle == 'MI':
                hm = formatted_time[8:12]
                new_dict = {"hm": hm}
            elif interface_cycle == 'HH':
                hh = formatted_time[8:10]
                new_dict = {"hh": hh}
            partition_arr.append(new_dict)

        return partition_arr

    @classmethod
    def _process_non_dt(cls, partition:Dict[str, str], filenm):
        partition_arr = []
        value = partition[list(partition.keys())[0]]

        if cls._has_group_capture(value):
            try:
                re.compile(value)
                logger.info("Regex: %s", value)
                nm_match = re.search(value, filenm)
                named_groups = nm_match.groupdict()
                partition_name = ""

                for name in named_groups:
                    partition_name += name

                if not partition_name:
                    logger.error("No regex result")
                    raise Exception("No regex result")
                else:
                    partition[list(partition.keys())[0]] = partition_name
                    partition_arr.append(partition)
            except Exception:
                logger.error("regex exception")
        else:
            partition_arr.append(partition)

        return partition_arr

    @classmethod
    def get_oracle_local_info_file_path(cls, interface_id):
        new_ulid = ulid.new()
        localInfoFilePath = '/data/gw_meta/' + interface_id + '/' + str(new_ulid) + ".dat"

        return localInfoFilePath

    @classmethod
    def get_oracle_file_name(cls, file_info, taget_time):
        tb_nm = file_info.db_proto_info.table_nm
        file_name = tb_nm + '_' + taget_time + '_'

        return file_name

    @classmethod
    def get_zip_path(cls, local_tmp, filenm):
        return str(Path(local_tmp, filenm))

    @classmethod
    def get_hdfs_path(cls, partitions, file_info):

        result_hdfs_path = "/idcube_out" + file_info.hdfs_dir
        for item in partitions:
            key = list(item.keys())[0]
            value = item[key]
            result_hdfs_path += f"/{key}={value}"
        return result_hdfs_path

    @classmethod
    def _get_dt_path(cls, target_time, interface_cycle, dbconn=None):
        dictArr = []
        target_time = cls._get_target_time(target_time)

        formatted_time = Util.get_target_filenm(interface_cycle, target_time)
        dt = formatted_time[:8]
        dictArr.append({"dt": dt})

        if len(formatted_time) > 8:
            time_suffix = cls._get_time_suffix(formatted_time, interface_cycle, dbconn)
            if time_suffix:
                dictArr.append(time_suffix)

        return dictArr

    @classmethod
    def _get_target_time(cls, target_time):
        format_mappings = {
            6: "%Y%m",
            8: "%Y%m%d",
            10: "%Y%m%d%H",
            12: "%Y%m%d%H%M"
        }
        date_format = format_mappings.get(len(target_time))
        if not date_format:
            raise ValueError(f"Invalid current_time length: {len(target_time)}")
        return datetime.strptime(target_time, date_format)

    @classmethod
    def _get_time_suffix(cls, formatted_time, interface_cycle, dbconn):
        if interface_cycle == 'MI':
            hm = formatted_time[8:12] if dbconn is None else formatted_time[8:]
            return {"hm": hm}
        elif interface_cycle == 'HH':
            return {"hh": formatted_time[8:10]}
        return None

    @classmethod
    def create_non_partitioned_path(cls, file_info):
        partDict = {}
        partDict['partitions'] = []
        logger.info("removed partitions..")
        partDict["hdfsPath"] = "/idcube_out" + file_info.hdfs_dir
        logger.info("non partition hdfs path : %s", file_info.hdfs_dir)
        return partDict

    @classmethod
    def remove_date_filenm(cls, filenm):
        re_pattern = r'[^a-zA-Z0-9]+(\d{6,})'
        filenm = re.sub(re_pattern, '', filenm)
        logger.info("removed date at filename : %s", filenm)
        return filenm

    @classmethod
    def extract_dir_and_combine_to_filenm(cls, meta_info, filenm):
        pattern = meta_info.ftp_proto_info.dir_extract_pattern.encode().decode('unicode_escape')
        target = meta_info.ftp_proto_info.source_dir

        logger.info("extract_pattern pattern: %s, target: %s", pattern, target)
        matches = re.search(pattern, target)

        if matches:
            filenm = matches.group(1) + "_" + filenm
            logger.info("extract_pattern filenm: %s", filenm)
            return filenm
        else:
            logger.error("extract_pattern no matches")
            return filenm

