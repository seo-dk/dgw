import logging
import os
import re
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass
from typing import Dict

import ulid
from airflow.models import Variable

from commons_test.MetaInfoHook import InterfaceInfo, SourceInfo
from commons_test.Util import Util

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
            local_path.local_info_data_path = Path("/data/gw_meta/test/", interface_info.interface_id, "info_" + str(new_ulid) + ".dat")

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
        partition_info = PartitionInfos()

        dictArr = []

        if source_info.file_proto_info:
            if source_info.file_proto_info.partitions:
                dictArr.extend(cls._process_partitions(source_info, target_time, interface_cycle, filename, started_time))
            else:
                dictArr.extend(cls._get_dt_path(target_time, interface_cycle))
        elif source_info.kafka_proto_info:
            if source_info.kafka_proto_info.partitions:
                dictArr.extend(cls._process_partitions(source_info, target_time, interface_cycle, filename, started_time))
            else:
                dictArr.extend(cls._get_dt_path(target_time, interface_cycle))
        else:
            dictArr.extend(cls._get_dt_path(target_time, interface_cycle, True))

        hdfsPath = cls.get_hdfs_path(dictArr, source_info)

        partDict["partitions"] = dictArr
        partition_info.partitions = dictArr

        partDict["hdfsPath"] = hdfsPath
        partition_info.hdfs_path = hdfsPath

        return partDict
        # return partition_info

    @classmethod
    def get_distcp_batch_partition_segments(cls, source_info: SourceInfo, interface_info: InterfaceInfo, formatted_start_time, filename: str = ""):
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
    def _process_partitions(cls, file_info: SourceInfo, target_time, interface_cycle, filename, started_time):
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

                if partition_name is None:
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
        localInfoFilePath = '/data/gw_meta/test/' + interface_id + '/' + str(new_ulid) + ".dat"

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
        result_hdfs_path = "/test/idcube_out" + file_info.hdfs_dir

        # TPDO-490
        # if file_info.noti_allow:
        #     result_hdfs_path = "/test/idcube_out" + file_info.hdfs_dir
        # else:
        #     result_hdfs_path = "/test/provide" + file_info.hdfs_dir

        for item in partitions:
            key = list(item.keys())[0]
            value = item[key]
            result_hdfs_path += f"/{key}={value}"
        return result_hdfs_path

    @classmethod
    def _get_dt_path(cls, target_time, interface_cycle, dbconn=None):
        logger.info("Came to here")
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
        partDict["hdfsPath"] = "/test/idcube_out" + file_info.hdfs_dir
        logger.info("non partition hdfs path : %s", file_info.hdfs_dir)
        return partDict

    @classmethod
    def remove_date_filenm(cls, filenm):
        re_pattern = r'[^a-zA-Z0-9]+(\d{6,})'
        filenm = re.sub(re_pattern, '', filenm)
        logger.info("removed date at filename : %s", filenm)
        return filenm

    @classmethod
    def align_filenm_with_partition(cls, file_nm, local_path_infos: LocalPathInfos, partitions):
        """
        partitions에 맞게 파일명을 수정하고 local_path_infos를 업데이트합니다.
        partitions에 dt만 있으면 파일명에서 hh, hm을 제거하고 dt까지만 남깁니다.
        
        Args:
            file_nm: 원본 파일명 (예: "data_2025042210.csv")
            local_path_infos: LocalPathInfos 객체
            partitions: partitions 딕셔너리 (예: {"partitions": [{"dt": "20250422"}]})
        
        Returns:
            수정된 LocalPathInfos 객체
        """
        if not partitions or not partitions.get("partitions"):
            return local_path_infos
        
        # partitions 리스트를 딕셔너리로 변환
        partition_dict = {}
        for item in partitions.get("partitions", []):
            if isinstance(item, dict):
                partition_dict.update(item)
        
        dt_value = partition_dict.get("dt")
        hh_value = partition_dict.get("hh")
        hm_value = partition_dict.get("hm")
        
        # dt가 없으면 원본 파일명 반환
        if not dt_value:
            logger.info("No dt in partitions, returning original filename: %s", file_nm)
            return local_path_infos
        
        # 파일명에서 날짜/시간 패턴 찾기 (YYYYMMDD, YYYYMMDDHH, YYYYMMDDHHMM, YYYYMMDDHHMMSS 등)
        patterns = [
            r'(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})',  # YYYYMMDDHHMMSS
            r'(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})',         # YYYYMMDDHHMM
            r'(\d{4})(\d{2})(\d{2})(\d{2})',                 # YYYYMMDDHH
            r'(\d{4})(\d{2})(\d{2})',                         # YYYYMMDD
        ]
        
        current_filename = os.path.basename(local_path_infos.local_tmp_full_path)
        aligned_filename = current_filename
        
        # current_filename에서 날짜/시간 패턴 찾기
        for pattern in patterns:
            match = re.search(pattern, current_filename)
            if match:
                matched_str = match.group(0)
                # partitions에 dt만 있으면 dt까지만 사용
                if dt_value and not hh_value and not hm_value:
                    # 파일명에서 dt까지만 남기고 나머지 제거
                    aligned_filename = current_filename.replace(matched_str, dt_value)
                    logger.info("Aligned filename: %s -> %s (partitions: dt=%s)", current_filename, aligned_filename, dt_value)
                    break
                # partitions에 hh가 있으면 dt+hh까지만 사용
                elif dt_value and hh_value and not hm_value:
                    dt_hh = dt_value + hh_value
                    aligned_filename = current_filename.replace(matched_str, dt_hh)
                    logger.info("Aligned filename: %s -> %s (partitions: dt=%s, hh=%s)", current_filename, aligned_filename, dt_value, hh_value)
                    break
                # partitions에 hm이 있으면 dt+hm까지만 사용
                elif dt_value and hm_value:
                    dt_hm = dt_value + hm_value
                    aligned_filename = current_filename.replace(matched_str, dt_hm)
                    logger.info("Aligned filename: %s -> %s (partitions: dt=%s, hm=%s)", current_filename, aligned_filename, dt_value, hm_value)
                    break
        
        # 파일명이 변경되었으면 local_tmp_full_path 업데이트
        if aligned_filename != current_filename:
            dir_path = os.path.dirname(local_path_infos.local_tmp_full_path)
            local_path_infos.local_tmp_full_path = Path(dir_path, aligned_filename)
            logger.info("Updated local_tmp_full_path: %s", local_path_infos.local_tmp_full_path)
        else:
            logger.info("No date pattern found in filename, keeping original: %s", current_filename)
        
        return local_path_infos

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

    @classmethod
    def get_partition_dict_list(cls, local_file_path):
        partition_dict_list = []
        start_match = re.search(r'.*/db=[^/]+/tb=[^/]+/', local_file_path)
        start_fix = None
        if start_match:
            start_fix = start_match.group(0)
        end_fix = "/"+os.path.basename(local_file_path)
        if start_fix and end_fix:
            pattern = re.escape(start_fix) + r'(.*)' + re.escape(end_fix)
            match = re.search(pattern, local_file_path)
            if match:
                partition_path = match.group(1)
                partition_path_arr = partition_path.split("/")
                for partition_path_str in partition_path_arr:
                    partition_dict = {}
                    partitions = partition_path_str.split("=")
                    if len(partitions) < 2:
                        continue
                    partition_dict[partitions[0]] = partitions[1]
                    partition_dict_list.append(partition_dict)
            else:
                logger.info("no patition pattern match")
        else:
            logger.info("no /idcube_out/db=*/tb=* pattern and file name in local_file_path")
        return partition_dict_list

def test_process_partitions():
    ## _process_partitions(cls, file_info: SourceInfo, target_time, interface_cycle, filename, started_time)
    ## started_time = 202504211712
    ## interface_cycle = [MM,DD,HH,MI]
    ## target_time = 202504221038
    ## filename = "20250422_test.dat
    target_time = datetime.now().strftime("%Y%m%d%H%M")
    started_time = 202504211712
    interface_cycle = ["MM", "DD", "HH", "MI"]
    partitions = [
        [{"dt": "${yyyyMMdd}"}, {"hh": "${HH}"}],
        [{"dt": "${yyyyMMdd}"}, {"hm": "${HHmm}"}],
        [{"dt": "${yyyyMMdd}"}, {"hm": "${HHmm}"}],
        [{"dt": "${yyyyMMdd}"}],
        [{"dt": "${yyyyMMdd-2d}"}],
        [{"dt": "${yyyyMMdd-2d}"}],
        [{"nw": "4g"}]
    ]

    dict_arr = []
    dt_exist = False
    partitions = None

    date_keys = ["dt", "hh", "hm"]

    if file_info.kafka_proto_info:
        partitions = file_info.kafka_proto_info.partitions
    if file_info.file_proto_info:
        partitions = file_info.file_proto_info.partitions

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


    print("test _process_partitions()")

def test_process_dt():
    ## partition, key, interface_cycle, started_time
    partitions_list = [
        [{"dt": "${yyyyMMdd}"}, {"hh": "${HH}"}],
        [{"dt": "${yyyyMMdd}"}, {"hm": "${HHmm}"}],
        [{"dt": "${yyyyMMdd}"}, {"hm": "${HHmm}"}],
        [{"dt": "${yyyyMMdd}"}],
        [{"dt": "${yyyyMMdd-2d}"}],
        [{"dt": "${yyyyMMdd-2d}"}],
        [{"nw": "4g"}]
    ]

    date_keys = ["dt", "hh", "hm"]
    target_time = datetime.now().strftime("%Y%m%d%H%M")
    started_time = datetime.now().strftime("%Y%m%d%H%M")
    interface_cycle = ["MM", "DD", "HH", "MI"]

    for partitions in partitions_list:
        for partition in partitions:
            key = list(partition.keys())[0]
            if key in date_keys:
                _process_dt()
                logger.info("started_time : %s", started_time)
                partition_arr = []
                time_pattern = partition[key]
                logger.info("partition time_pattern : %s", time_pattern)
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


    print("test _process_dt()")

def test_process_dt():
    print("test _process_dt()")


