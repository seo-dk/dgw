import logging
import os
import re
from datetime import datetime
from pathlib import Path

import ulid
from airflow.models import Variable

from commons_test.Parse import Parse

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))


class SetPath():
    def setFTPLocalPath(self, meta_info, start_time):
        path_dict = {}
        if isinstance(start_time, str):
            start_time_str = start_time
        else:
            start_time_str = start_time.strftime("%Y%m%d%H")
        dt = start_time_str[:8]
        hh = start_time_str[8:]
        local_tmp_path = Path("/data", "/idcube_out", meta_info.INTERFACE_ID)
        path_dict['localTmpPath'] = local_tmp_path

        if not os.path.exists(str(local_tmp_path)):
            os.makedirs(str(local_tmp_path))

        new_ulid = ulid.new()
        path_dict['infoDatPath'] = Path(local_tmp_path, "info_" + str(new_ulid) + ".dat")

        return path_dict

    def setFTPFullPath_v4(self, meta_info, file_path, match_file_nm, info_dat_path, os_file_path=None):
        path_dict = {}
        if match_file_nm:
            logger.info("Matched file name : %s", match_file_nm)
            if os_file_path is not None:
                source_file_path = os_file_path
            else:
                source_file_path = Path(str(meta_info.FTP_SERVER_INFO.DATA_SOURCE_DIR), match_file_nm)
                logger.info("Source file path : %s", source_file_path)

            if not meta_info.FTP_SERVER_INFO.USE_PARTITIONS:
                filenm = self.remove_date_filenm(match_file_nm)
            else:
                filenm = os.path.basename(source_file_path)

            path_dict['sourceFilePath'] = source_file_path
            tmp_path = "/data" + str(file_path)
            logger.info("tmp_path : %s", tmp_path)
            if not os.path.exists(tmp_path):
                os.makedirs(str(tmp_path))
            tmp_full_path = Path(tmp_path, filenm)
            logger.info("local tmp full path : %s", tmp_full_path)
            path_dict['tmpPath'] = tmp_path
            path_dict['tmpFullPath'] = tmp_full_path
            new_ulid = ulid.new()
            current_time = datetime.now().strftime("%Y%m%d%H")
            if info_dat_path is not None:
                path_dict['infoDatPath'] = info_dat_path
            else:
                path_dict['infoDatPath'] = Path("/data/gw_meta", meta_info.INTERFACE_ID, "info_" + str(new_ulid) + ".dat")
        else:
            tmp_path = "/data" + str(file_path)
            path_dict['tmpPath'] = tmp_path
            if info_dat_path is not None:
                path_dict['infoDatPath'] = info_dat_path

        return path_dict

    def _has_group_capture(self, regex):
        pattern = re.compile(r'\([^)]*\)')

        # 괄호가 있는지 확인
        if pattern.search(regex):
            return True
        else:
            return False

    def setPartitionInfo(self, file_info, target_time, interface_cycle, filenm, started_time):
        parse = Parse()

        partDict = {}
        dictArr = []

        if hasattr(file_info, 'FILE_INFO') and file_info.FILE_INFO:
            if file_info.FILE_INFO.PARTITIONS:
                dictArr.extend(self._process_partitions(parse, file_info, target_time, interface_cycle, filenm, started_time))
            else:
                dictArr.extend(self.set_dt_path(parse, target_time, interface_cycle))
        else:
            dictArr.extend(self.set_dt_path(parse, target_time, interface_cycle, True))

        hdfsPath = self.set_hdfs_path(dictArr, file_info)

        partDict["partitions"] = dictArr
        partDict["hdfsPath"] = hdfsPath

        return partDict

    def _process_partitions(self, parse, file_info, target_time, interface_cycle, filenm, started_time):
        dict_arr = []
        dt_exist = False

        for dict in file_info.FILE_INFO.PARTITIONS:
            logger.info("dict : %s", dict)
            key = list(dict.keys())[0]
            if key == "dt":
                dict_arr.extend(self._process_dt(parse, dict, interface_cycle, started_time))
                dt_exist = True
            else:
                dict_arr.extend(self._process_non_dt(dict, filenm))

        if not dt_exist:
            dict_arr = self.set_dt_path(parse, target_time, interface_cycle) + dict_arr

        return dict_arr

    def _process_dt(self, parse, dict, interface_cycle, started_time):
        logger.info("started_time : %s",started_time)
        dict_arr = []
        time_pattern = dict["dt"]
        if parse.getTimePattern(time_pattern):
            formatted_time = parse.get_calculate_pattern(time_pattern, started_time)
        else:
            formatted_time = dict["dt"]
        logger.info("partition dt time : %s", formatted_time)

        dt = formatted_time[:8]
        dict["dt"] = dt
        dict_arr.append(dict)

        if len(formatted_time) > 8:
            new_dict = {}
            if interface_cycle == 'MI':
                hm = formatted_time[8:12]
                new_dict = {"hm": hm}
            elif interface_cycle == 'HH':
                hh = formatted_time[8:10]
                new_dict = {"hh": hh}
            dict_arr.append(new_dict)

        return dict_arr

    def _process_non_dt(self, dict, filenm):
        dictArr = []
        value = dict[list(dict.keys())[0]]

        if self._has_group_capture(value):
            try:
                re.compile(value)
                logger.info("Regex: %s", value)
                nm_match = re.search(value, filenm)
                named_groups = nm_match.groupdict()
                partition_name = None

                for name in named_groups:
                    partition_name += name

                if partition_name is None:
                    logger.error("No regex result")
                    raise Exception("No regex result")
                else:
                    dict[list(dict.keys())[0]] = partition_name
                    dictArr.append(dict)
            except Exception:
                logger.error("regex exception")
        else:
            dictArr.append(dict)

        return dictArr

    def setOracleLocalInfoFilePath(self, INTERFACE_ID):
        new_ulid = ulid.new()
        localInfoFilePath = '/data/gw_meta/' + INTERFACE_ID + '/' + str(new_ulid) + ".dat"

        return localInfoFilePath

    def set_oracle_local_Info_file_path_test(self, INTERFACE_ID):
        new_ulid = ulid.new()
        localInfoFilePath = '/data/gw_meta/test/' + INTERFACE_ID + '/' + str(new_ulid) + ".dat"

        return localInfoFilePath

    def setOracleFileName(self, file_info, taget_time):
        tb_nm = file_info.DB_QUERY_INFO.table_nm
        file_name = tb_nm + '_' + taget_time + '_'

        return file_name

    def setZipPath(self, local_tmp, filenm):
        return str(Path(local_tmp, filenm))

    def set_hdfs_path(self, partitions, file_info):

        result_hdfs_path = "/test/idcube_out" + file_info.HDFS_DIR
        for item in partitions:
            key = list(item.keys())[0]
            value = item[key]
            result_hdfs_path += f"/{key}={value}"
        return result_hdfs_path

    def set_dt_path(self, parse, target_time, interface_cycle, dbconn=None):
        dictArr = []
        target_time = self._get_target_time(target_time)

        formatted_time = parse.setTargetFilenm(interface_cycle, target_time)
        dt = formatted_time[:8]
        dictArr.append({"dt": dt})

        if len(formatted_time) > 8:
            time_suffix = self._get_time_suffix(formatted_time, interface_cycle, dbconn)
            if time_suffix:
                dictArr.append(time_suffix)

        return dictArr

    def _get_target_time(self, target_time):
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

    def _get_time_suffix(self, formatted_time, interface_cycle, dbconn):
        if interface_cycle == 'MI':
            hm = formatted_time[8:12] if dbconn is None else formatted_time[8:]
            return {"hm": hm}
        elif interface_cycle == 'HH':
            return {"hh": formatted_time[8:10]}
        return None

    def create_non_partitioned_path(self, file_info, filenm):
        partDict = {}
        partDict['partitions'] = []
        logger.info("removed partitions..")
        partDict["hdfsPath"] = "/test/idcube_out" + file_info.HDFS_DIR
        logger.info("non partition hdfs path : %s", file_info.HDFS_DIR)
        return partDict

    def remove_date_filenm(self, filenm):
        re_pattern = r'[^a-zA-Z0-9]+(\d{6,})'
        filenm = re.sub(re_pattern, '', filenm)
        logger.info("removed date at filenm : %s", filenm)
        return filenm

    def extract_dir_and_combine_to_filenm(self, meta_info, filenm):
        pattern = meta_info.FTP_SERVER_INFO.DIR_EXTRACT_PATTERN.encode().decode('unicode_escape')
        target = meta_info.FTP_SERVER_INFO.DATA_SOURCE_DIR

        logger.info("extract_pattern pattern: %s, target: %s", pattern, target)
        matches = re.search(pattern, target)

        if matches:
            filenm = matches.group(1) + "_" + filenm
            logger.info("extract_pattern filenm: %s", filenm)
            return filenm
        else:
            logger.error("extract_pattern no matches")
            return filenm
