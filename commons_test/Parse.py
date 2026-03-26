import logging
import operator
import os
from datetime import datetime, timedelta

from airflow.models import Variable
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))


class Parse():
    def __init__(self):
        import re
        self.re = re
        self.targetTime = None

    def _time_calculate(self, cal_num, unit, start_time):
        time = datetime.strptime(start_time,"%Y%m%d%H%M")
        if unit == 'm':
            cal_time = time + timedelta(minutes=cal_num)
        elif unit == 'H':
            cal_time = time + timedelta(hours=cal_num)
        elif unit == 'd':
            cal_time = time + timedelta(days=cal_num)
        elif unit == 'M':
            cal_time = time + relativedelta(months=cal_num)
        return cal_time


    def getTimePattern(self, file_pattern):
        r_p1 = r'\${.*?}'
        # find time pattern
        timepattern = self.re.findall(r_p1, file_pattern)
        return timepattern

    def checkTimePattern(self, pattern, target_time):
        timeStr = None
        if pattern == "${yyyy}":
            timeStr = target_time.strftime("%Y")
        elif pattern == "${MM}":
            timeStr = target_time.strftime("%m")
        elif pattern == "${dd}":
            timeStr = target_time.strftime("%d")
        elif pattern == "${HH}":
            timeStr = target_time.strftime("%H")
        elif pattern == "${yyyyMM}":
            timeStr = target_time.strftime("%Y%m")
        elif pattern == "${yyyyMMdd}":
            timeStr = target_time.strftime("%Y%m%d")
        elif pattern == "${yyyyMMddHH}":
            timeStr = target_time.strftime("%Y%m%d%H")
        elif pattern == "${yyyyMMdd_HH}":
            timeStr = target_time.strftime("%Y%m%d_%H")
        elif pattern == "${yyyyMMdd HH}":
            timeStr = target_time.strftime("%Y%m%d%H")
        elif pattern == "${yyyyMMdd_HHmm}":
            timeStr = target_time.strftime("%Y%m%d_%H%M")
        elif pattern == "${yyyyMMdd HHmm}":
            timeStr = target_time.strftime("%Y%m%d%H%M")
        elif pattern == "${yyyyMMddHHmm}":
            timeStr = target_time.strftime("%Y%m%d%H%M")
        return timeStr
    def get_calculate_pattern(self, pattern, started_time):
        calculate_regix = r"([+-])(\d+)([dHMm])"
        time_pattern_regix = r"[+-]\d+[dHMm]"
        cal = self.re.search(calculate_regix, pattern)
        time_pattern = self.re.sub(time_pattern_regix, "", pattern)
        if cal:
            logger.info("date calculate start...")
            sign, number, unit = cal.groups()
            cal_num = int(number) * (-1 if sign == '-' else 1)
            calculate_time = self._time_calculate(cal_num, unit, started_time)
            logger.info("calculated date : %s",calculate_time)
            pattern_to_time = self.checkTimePattern(time_pattern, calculate_time)
            logger.info("formatted date: %s",pattern_to_time)
            return pattern_to_time
        else:
            date_start_time = datetime.strptime(started_time,"%Y%m%d%H%M")
            pattern_to_time = self.checkTimePattern(time_pattern, date_start_time)
            logger.info("formatted date: %s", pattern_to_time)
            return pattern_to_time
    def getTargetTime_v2(self, time_pattern, file_info, interface_cycle, start_time, target_time=None):
        resultTime = None
        time_dict = {}
        for pattern in time_pattern:
            calculate_regix = r"([+-])(\d+)([dHMm])"
            time_pattern_regix = r"-\d+[dHMm]"
            cal = self.re.search(calculate_regix, pattern)
            no_cal_time_pattern = self.re.sub(time_pattern_regix, "", pattern)
            if target_time:
                # target time
                date_obj = datetime.strptime(target_time, "%Y%m%d%H%M")
                pattern_to_time = self.checkTimePattern(no_cal_time_pattern, date_obj)
                time_dict['change_nm'] = file_info.replace(pattern, pattern_to_time)
                time_dict['target_time'] = date_obj.strftime("%Y%m%d%H%M")
                return time_dict
            else:
                if cal:
                    sign, number, unit = cal.groups()
                    cal_num = int(number) * (-1 if sign == '-' else 1)
                    cal_time = self._time_calculate(cal_num, unit, start_time)
                    pattern_to_time = self.checkTimePattern(no_cal_time_pattern, cal_time)
                    time_dict['change_nm'] = file_info.replace(pattern, pattern_to_time)
                    time_dict['target_time'] = cal_time.strftime("%Y%m%d%H%M")
                    return time_dict
                else:
                    # target time
                    logger.info("Time pattern : %s",no_cal_time_pattern)
                    date_start_time = datetime.strptime(start_time,"%Y%m%d%H%M")
                    pattern_to_time = self.checkTimePattern(no_cal_time_pattern, date_start_time)
                    time_dict['change_nm'] = file_info.replace(pattern, pattern_to_time)
                    time_dict['target_time'] = date_start_time.strftime("%Y%m%d%H%M")
                    return time_dict

    def setTargetFilenm(self, interface_cycle, currrent_time):
        try:
            # currrent_time 202308221800 type is datetime
            formatted_time = None
            if interface_cycle == "MI":
                formatted_time = currrent_time.strftime("%Y%m%d%H%M")
            elif interface_cycle == "HH":
                formatted_time = currrent_time.strftime("%Y%m%d%H")
            elif interface_cycle == "DD":
                formatted_time = currrent_time.strftime("%Y%m%d")
            elif interface_cycle == "MM":
                formatted_time = currrent_time.strftime("%Y%m")
            elif interface_cycle == "WW":
                formatted_time = currrent_time.strftime("%Y%m%d")

            # formatted_time type is str
            return formatted_time

        except Exception as e:
            logger.exception("setTargetFilenm error: %s", e)

    def parseNoti(self, interface_info,target_time):

        notiDirPattern = self.getTimePattern(interface_info.FTP_SERVER_INFO.NOTI_DIR)
        notiFilePattern = self.getTimePattern(interface_info.FTP_SERVER_INFO.noti_file_nm)

        if notiDirPattern:
            time_dict = self.getTargetTime_v2(notiDirPattern, interface_info.FTP_SERVER_INFO.NOTI_DIR,
                                              interface_info.INTERFACE_CYCLE,target_time)
            interface_info.FTP_SERVER_INFO.NOTI_DIR = time_dict['change_nm']
        if notiFilePattern:
            time_dict = self.getTargetTime_v2(notiFilePattern, interface_info.FTP_SERVER_INFO.noti_file_nm,
                                              interface_info.INTERFACE_CYCLE, target_time)
            interface_info.FTP_SERVER_INFO.noti_file_nm = time_dict['change_nm']

    def replaceTimePattern_v2(self, file_list_info, interface_info, target_time=None):
        current_time = datetime.now()
        for fileInfo in file_list_info:
            ##get time pattern at file data_source_file_nm
            fileSourcePattern = self.getTimePattern(fileInfo.FILE_INFO.DATA_SOURCE_FILE_NM)
            fileNmPattern = self.getTimePattern(fileInfo.FILE_INFO.FILE_NM)
            if fileSourcePattern is not None or fileNmPattern is not None:
                # enter when it's retry
                if target_time:
                    logger.info("target time is : %s",target_time)
                    if fileNmPattern:
                        time_dict = self.getTargetTime_v2(fileNmPattern, fileInfo.FILE_INFO.FILE_NM,
                                                          interface_info.INTERFACE_CYCLE, interface_info.start_time, target_time)
                        fileInfo.FILE_INFO.FILE_NM = time_dict['change_nm']
                        fileInfo.FILE_INFO.TARGET_TIME = time_dict['target_time']
                    if fileSourcePattern:
                        time_dict = self.getTargetTime_v2(fileSourcePattern, fileInfo.FILE_INFO.DATA_SOURCE_FILE_NM,
                                                          interface_info.INTERFACE_CYCLE, interface_info.start_time, target_time)
                        fileInfo.FILE_INFO.DATA_SOURCE_FILE_NM = time_dict['change_nm']
                        if fileInfo.FILE_INFO.TARGET_TIME is None:
                            fileInfo.FILE_INFO.TARGET_TIME = time_dict['target_time']
                else:
                    if fileNmPattern:
                        time_dict = self.getTargetTime_v2(fileNmPattern, fileInfo.FILE_INFO.FILE_NM,
                                                          interface_info.INTERFACE_CYCLE, interface_info.start_time)
                        fileInfo.FILE_INFO.FILE_NM = time_dict['change_nm']
                        fileInfo.FILE_INFO.TARGET_TIME = time_dict['target_time']
                    if fileSourcePattern:
                        time_dict = self.getTargetTime_v2(fileSourcePattern, fileInfo.FILE_INFO.DATA_SOURCE_FILE_NM,
                                                          interface_info.INTERFACE_CYCLE, interface_info.start_time)
                        fileInfo.FILE_INFO.DATA_SOURCE_FILE_NM = time_dict['change_nm']
                        if fileInfo.FILE_INFO.TARGET_TIME is None:
                            fileInfo.FILE_INFO.TARGET_TIME = time_dict['target_time']

            logger.info(fileInfo)

        interface_filenm_pattern = self.getTimePattern(interface_info.FTP_SERVER_INFO.DATA_SOURCE_FILE_NM)
        sourceDirTimePattern = self.getTimePattern(interface_info.FTP_SERVER_INFO.DATA_SOURCE_DIR)
        if sourceDirTimePattern is not None or interface_filenm_pattern is not None:
            if target_time:
                if interface_filenm_pattern:
                    time_dict = self.getTargetTime_v2(interface_filenm_pattern,
                                                      interface_info.FTP_SERVER_INFO.DATA_SOURCE_FILE_NM,
                                                      interface_info.INTERFACE_CYCLE, interface_info.start_time,
                                                      target_time)
                    interface_info.FTP_SERVER_INFO.DATA_SOURCE_FILE_NM = time_dict['change_nm']
                    interface_info.TARGET_TIME = time_dict['target_time']
                    for fileInfo in file_list_info:
                        if fileInfo.FILE_INFO.TARGET_TIME is None:
                            fileInfo.FILE_INFO.TARGET_TIME = time_dict['target_time']
                if sourceDirTimePattern:
                    time_dict = self.getTargetTime_v2(sourceDirTimePattern,
                                                      interface_info.FTP_SERVER_INFO.DATA_SOURCE_DIR,
                                                      interface_info.INTERFACE_CYCLE, interface_info.start_time,
                                                      target_time)
                    interface_info.FTP_SERVER_INFO.DATA_SOURCE_DIR = time_dict['change_nm']
                    if interface_info.TARGET_TIME is None:
                        interface_info.TARGET_TIME = time_dict['target_time']
                    for fileInfo in file_list_info:
                        if fileInfo.FILE_INFO.TARGET_TIME is None:
                            fileInfo.FILE_INFO.TARGET_TIME = time_dict['target_time']
            else:
                if interface_filenm_pattern:
                    time_dict = self.getTargetTime_v2(interface_filenm_pattern,
                                                      interface_info.FTP_SERVER_INFO.DATA_SOURCE_FILE_NM,
                                                      interface_info.INTERFACE_CYCLE, interface_info.start_time)
                    interface_info.FTP_SERVER_INFO.DATA_SOURCE_FILE_NM = time_dict['change_nm']
                    interface_info.TARGET_TIME = time_dict['target_time']
                    for fileInfo in file_list_info:
                        if fileInfo.FILE_INFO.TARGET_TIME is None:
                            fileInfo.FILE_INFO.TARGET_TIME = time_dict['target_time']
                if sourceDirTimePattern:
                    time_dict = self.getTargetTime_v2(sourceDirTimePattern,
                                                      interface_info.FTP_SERVER_INFO.DATA_SOURCE_DIR,
                                                      interface_info.INTERFACE_CYCLE, interface_info.start_time)
                    interface_info.FTP_SERVER_INFO.DATA_SOURCE_DIR = time_dict['change_nm']
                    if interface_info.TARGET_TIME is None:
                        interface_info.TARGET_TIME = time_dict['target_time']
                    for fileInfo in file_list_info:
                        if fileInfo.FILE_INFO.TARGET_TIME is None:
                            fileInfo.FILE_INFO.TARGET_TIME = time_dict['target_time']

            check_target_time = True
            for fileInfo in file_list_info:
                if fileInfo.FILE_INFO.TARGET_TIME is not None:
                    check_target_time = False
                    break
            if check_target_time and interface_info.TARGET_TIME is None:
                for fileInfo in file_list_info:
                    fileInfo.FILE_INFO.TARGET_TIME = current_time.strftime("%Y%m%d%H%M")
                interface_info.TARGET_TIME = current_time.strftime("%Y%m%d%H%M")

    # Oracle
    def getTimeValue(self, time_pattern, time_value, operator_str):
        # list = [1y, 1M, 1d, 10m]
        time_value = datetime.strptime(time_value, "%Y%m%d%H%M")

        operator_mapping = {
            "+": operator.add,
            "-": operator.sub
        }

        operator_func = None

        if operator_str == '+':
            operator_func = operator_mapping['+']
        else:
            operator_func = operator_mapping['-']

        logger.debug("getTimeValue timePattern : %s", time_pattern)

        for value in time_pattern:
            numList = self.re.findall(r'\d+', value)
            num = int(numList[0])

            charList = self.re.findall(r'[a-zA-Z]', value)
            char = charList[0]

            if char == "y":
                time_value = operator_func(time_value, relativedelta(years=num))
            elif char == "M":
                time_value = operator_func(time_value, relativedelta(months=num))
            elif char == "d":
                time_value = operator_func(time_value, timedelta(days=num))
            elif char == "H":
                time_value = operator_func(time_value, timedelta(hours=num))
            elif char == "m":
                time_value = operator_func(time_value, timedelta(minutes=num))

        logger.debug("getTimeValue calculated timeValue: %s", time_value)

        result_time = None
        lastChar = time_pattern[-1]

        # default = yyyyMMdd
        if "y" in lastChar:
            result_time = time_value.strftime("%Y")
        elif "M" in lastChar:
            result_time = time_value.strftime("%Y%m")
        elif "d" in lastChar:
            result_time = time_value.strftime("%Y%m%d")
        elif "H" in lastChar:
            result_time = time_value.strftime("%Y%m%d%H")
        elif "m" in lastChar:
            result_time = time_value.strftime("%Y%m%d%H%M")

        if result_time is None:
            logger.exception("result_time is None")

        # result_time type is str
        return result_time

    # get param_result & target_time(fileName) .. replace '${yyyyMMddHH-10M}' -> 'realTime'
    def getSqlParams(self, querys_params, current_time):
        # querys_params = ["${yyyyMMddHH-10M}','${yyyyMMddHH}", "${yyyyMMddHH-3M}']
        param_reuslt = []

        for data_param in querys_params:
            if "$" in data_param:
                # querys_params = ${yyyyMMddHH-10M}
                logger.info("getSqlParams data_param: %s", data_param)
                if "-" in data_param:
                    # findall type list
                    matches = self.re.findall(r'\d+\w', data_param)
                    calculated_time = self.getTimeValue(matches, current_time, "-")
                    logger.info("getSqlParams calculated_time: %s", calculated_time)
                    param_reuslt.append(calculated_time)
                # querys_params = ${yyyyMMddHH}
                else:
                    formatted_time = self.checkTimePattern(data_param, datetime.strptime(current_time, "%Y%m%d%H%M"))
                    logger.info("getSqlParams formatted_time: %s", formatted_time)
                    param_reuslt.append(formatted_time)
            # if querys_params has no timePattern param
            else:
                logger.warn("querys_params has no time pattern: %s", data_param)
                param_reuslt.append(data_param)

        return param_reuslt

    def getQueryUsingRowNumDataQuery(self, data_query, start_row_num, end_row_num, max_row_count):
        if end_row_num == -1:
            end_row_num = max_row_count
        params = ["#", str(end_row_num), str(start_row_num), str(end_row_num), str(end_row_num)]

        if len(params) != data_query.count('#'):
            logger.error("data_query formatting fail len(params): %s, count('#'): %s", len(params), data_query.count('#'))

        formatted_sql = data_query.replace('#', '{}')
        formatted_sql = formatted_sql.format(*params)

        logger.debug("getQueryUsingRowNum params: %s", params)
        logger.debug("getQueryUsingRowNumDataQuery SQL: %s", formatted_sql)

        return formatted_sql

    def getQueryUsingRowNum(self, data_query, start_row_num, end_row_num):
        # select org.*
        #   from (select rownum as rn, CTIME from PM_LINK_ENB_KPI_1H WHERE EVENT_TIME=to_date('2023072010','yyyymmddhh24')) org
        #   WHERE rn between 3 and 10;

        select_index = data_query.lower().index("select")
        sub_query = "SELECT ROWNUM AS rn," + data_query[select_index + len("select"):]

        params = []
        params.append(sub_query)
        params.append(start_row_num)
        sql = "SELECT * FROM ({}) WHERE rn > {} AND rn <= {}"

        if end_row_num == -1:
            sql = "SELECT * FROM ({}) WHERE rn > {}"
        else:
            params.append(end_row_num)

        formatted_sql = sql.format(*params)
        logger.info("getQueryUsingRowNum SQL: %s", formatted_sql)

        return formatted_sql

    def getRowNumList(self, record_count, num_rows):
        row_num_list = []
        if num_rows > 0:
            quotient = record_count // num_rows
            logger.info("num_rows %s, record_count %s, quotient %s ", num_rows, record_count, quotient)

            for i in range(0, quotient + 1):
                row_num_list.append(i * num_rows)
        else:
            row_num_list.append(record_count)

            logger.info("row_num_list leng: %s", len(row_num_list))

        return row_num_list

    def getDataQueryList(self, row_num_list, num_rows, data_query, record_count):
        # according to num_rows, query_list size can be 1 or n
        query_list = []
        if num_rows > 0:
            for index in range(len(row_num_list)):
                start_row_num = row_num_list[index]

                if index == len(row_num_list) - 1:
                    end_row_num = -1
                else:
                    end_row_num = row_num_list[index + 1]

                # "C-TANGO_EC-DBCONN-%"
                query = None
                if "ROWNUM" in data_query:
                    query = self.getQueryUsingRowNumDataQuery(data_query, start_row_num, end_row_num, record_count)
                else:
                    query = self.getQueryUsingRowNum(data_query, start_row_num, end_row_num)
                query_list.append(query)
        else:
            query_list.append(data_query)

        return query_list

    # latest date is targetTime
    def getOracleTargetTime(self, param_dic):
        taget_time = param_dic[0]

        for item in param_dic:
            if item < taget_time:
                taget_time = item

        logger.info("partitions targetDT %s", taget_time)

        return taget_time

    def getFilenm(self, file_path):
        filenm = os.path.basename(file_path)

        datePattern = r"\d{6,}"
        filenm = self.re.sub(datePattern, "", filenm)
        extPtn = r"\.\w+$"
        filenm = self.re.sub(extPtn, "", filenm)
        rmUndB = r"^_|_$"
        filenm = self.re.sub(rmUndB, "", filenm)
        return filenm
