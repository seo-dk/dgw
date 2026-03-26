import logging
import operator
import os
from datetime import datetime, timedelta
import re
from pathlib import Path

from airflow.models import Variable
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))


class Util:

    @classmethod
    def apply_offset_from_match(cls, base_time_str, match):
        if not match:
            raise ValueError("Invalid time offset expression (match is None)")

        sign, number, unit = match.groups()
        offset = int(number) * (-1 if sign == '-' else 1)
        return cls._apply_time_offset(base_time_str, offset, unit)

    @classmethod
    def _apply_time_offset(cls, start_time, cal_num, unit):
        base_time = datetime.strptime(start_time, "%Y%m%d%H%M")

        time_units = {
            'm': timedelta(minutes=cal_num),
            'H': timedelta(hours=cal_num),
            'd': timedelta(days=cal_num),
            'M': relativedelta(months=cal_num),
            'y': relativedelta(years=cal_num),
        }

        if unit not in time_units:
            raise ValueError(f"Invalid time unit: {unit}")

        return base_time + time_units[unit]

    @classmethod
    def get_time_pattern(cls, file_pattern):
        return re.findall(r'\${.*?}', file_pattern)

    @classmethod
    def _format_time_pattern(cls, pattern, target_time):
        pattern_map = {
            "${yyyy}": "%Y",
            "${MM}": "%m",
            "${dd}": "%d",
            "${HH}": "%H",
            "${yyyyMM}": "%Y%m",
            "${yyyyMMdd}": "%Y%m%d",
            "${yyyyMMddHH}": "%Y%m%d%H",
            "${yyyyMMdd_HH}": "%Y%m%d_%H",
            "${yyyyMMdd HH}": "%Y%m%d%H",
            "${yyyyMMdd_HHmm}": "%Y%m%d_%H%M",
            "${yyyyMMdd HHmm}": "%Y%m%d%H%M",
            "${yyyyMMddHHmm}": "%Y%m%d%H%M",
            "${yyyyMM}01": "%Y%m01",
            "${yyMMddHH}": "%y%m%d%H",
            "${HHmm}": "%H%M"
        }

        if pattern not in pattern_map:
            raise ValueError(f"Invalid time pattern: {pattern} not found in pattern_map.")

        return target_time.strftime(pattern_map[pattern])

    @classmethod
    def get_calculate_pattern(cls, pattern, start_time):
        offset_regex = r"([+-])(\d+)([ydHMm])"
        offset_text_regex = r"[+-]\d+[ydHMm]"

        match = re.search(offset_regex, pattern)
        base_pattern = re.sub(offset_text_regex, "", pattern)

        if match:
            logger.info("Offset detected in pattern. Calculating date...")
            target_dt = cls.apply_offset_from_match(start_time, match)
            logger.info("Calculated datetime: %s", target_dt)
        else:
            target_dt = datetime.strptime(start_time, "%Y%m%d%H%M")
            logger.info("No offset found. Using base time: %s", target_dt)

        formatted = cls._format_time_pattern(base_pattern, target_dt)
        logger.info("Formatted time string: %s", formatted)
        return formatted

    @classmethod
    def get_target_time(cls, time_patterns, file_nm, start_time, target_time=None):
        time_info = {}
        modified_file_nm = file_nm

        for pattern in time_patterns:
            base_pattern, calculation = cls._extract_calculation_from_pattern(pattern)

            if target_time:
                time_info = cls._format_filename_with_time(modified_file_nm, pattern, target_time, base_pattern)
            else:
                time_info = cls._format_filename_with_time_offset(modified_file_nm, pattern, start_time, base_pattern, calculation)
                modified_file_nm = time_info['change_nm']

            logger.info("time_info : %s", time_info)

        return time_info

    @classmethod
    def _extract_calculation_from_pattern(cls, pattern):
        calculation_regex = r"([+-])(\d+)([ydHMm])"
        time_change_regex = r"[+-]\d+[ydHMm]"

        calculation = re.search(calculation_regex, pattern)
        base_pattern = re.sub(time_change_regex, "", pattern)

        return base_pattern, calculation

    @classmethod
    def _format_filename_with_time(cls, file_name, pattern, target_time_str, format_pattern):
        dt = datetime.strptime(target_time_str, "%Y%m%d%H%M")
        return {
            'change_nm': file_name.replace(pattern, cls._format_time_pattern(format_pattern, dt)),
            'target_time': target_time_str
        }

    @classmethod
    def _format_filename_with_time_offset(cls, modified_file_nm, pattern, start_time, base_pattern, calculation):
        if calculation:
            return cls._generate_filename_with_time_offset(modified_file_nm, pattern, start_time, base_pattern, calculation)
        else:
            return cls._generate_filename_with_start_time(modified_file_nm, pattern, start_time, base_pattern)

    @classmethod
    def _generate_filename_with_time_offset(cls, modified_file_nm, pattern, start_time, base_pattern, calculation):
        sign, number, unit = calculation.groups()
        calculated_number = int(number) * (-1 if sign == '-' else 1)
        calculated_time = cls._apply_time_offset(start_time, calculated_number, unit)
        formatted_time = cls._format_time_pattern(base_pattern, calculated_time)
        modified_file_nm = modified_file_nm.replace(pattern, formatted_time)
        return {
            'change_nm': modified_file_nm,
            'target_time': calculated_time.strftime("%Y%m%d%H%M")
        }

    @classmethod
    def _replace_regex_time_pattern(cls, file_name, pattern, start_time):
        regex_like_patterns = {
            '\\d{12}': 12,
            '\\d{10}': 10,
            '\\d{8}': 8,
            '\\d{6}': 6,
        }

        if pattern not in regex_like_patterns:
            raise ValueError(f"Unknown regex time pattern: {pattern}")

        length = regex_like_patterns[pattern]
        time_str = start_time[:length]
        return file_name.replace(pattern, time_str)

    @classmethod
    def _generate_filename_with_start_time(cls, modified_file_info, pattern, start_time, base_pattern):
        start_date_obj = datetime.strptime(start_time, "%Y%m%d%H%M")

        if re.fullmatch(r'\\d{\d+}', pattern):
            try:
                modified_file_info = cls._replace_regex_time_pattern(modified_file_info, pattern, start_time)
                return {
                    'change_nm': modified_file_info,
                    'target_time': start_time
                }
            except ValueError as e:
                logger.warning(str(e))
                raise

        if pattern.startswith("${"):
            formatted_time = cls._format_time_pattern(base_pattern, start_date_obj)
            modified_file_info = modified_file_info.replace(pattern, formatted_time)
            return {
                'change_nm': modified_file_info,
                'target_time': start_date_obj.strftime("%Y%m%d%H%M")
            }

        raise ValueError(f"Unsupported pattern format: {pattern}")

    @classmethod
    def get_target_filenm(cls, interface_cycle, current_time):
        cycle_format_map = {
            "MI": "%Y%m%d%H%M",
            "HH": "%Y%m%d%H",
            "DD": "%Y%m%d",
            "MM": "%Y%m",
            "WW": "%Y%m%d",
        }

        if interface_cycle in cycle_format_map:
            return current_time.strftime(cycle_format_map[interface_cycle])

        raise ValueError(f"Invalid interface cycle: {interface_cycle} not found in cycle_format_map.")

    @classmethod
    def parse_noti(cls, interface_info, target_time):

        noti_dir_pattern = cls.get_time_pattern(interface_info.ftp_proto_info.noti_dir)
        noti_file_pattern = cls.get_time_pattern(interface_info.ftp_proto_info.noti_file_nm)

        if noti_dir_pattern:
            time_dict = cls.get_target_time(noti_dir_pattern, interface_info.ftp_proto_info.noti_dir, target_time)
            interface_info.ftp_proto_info.noti_dir = time_dict['change_nm']
        if noti_file_pattern:
            time_dict = cls.get_target_time(noti_file_pattern, interface_info.ftp_proto_info.noti_file_nm, target_time)
            interface_info.ftp_proto_info.noti_file_nm = time_dict['change_nm']

    @classmethod
    def replace_time_pattern(cls, source_infos, interface_info, target_time=None):
        current_time = datetime.now()

        def _apply_time_pattern_to_source_info(file_nm_pattern, file_source_pattern, source_info, start_time, target_time):
            pattern = file_nm_pattern if file_nm_pattern is not None else file_source_pattern

            if file_nm_pattern:
                time_dict = cls.get_target_time(pattern, source_info.file_proto_info.file_nm, start_time, target_time)
                source_info.file_proto_info.file_nm = time_dict['change_nm']
                source_info.file_proto_info.target_time = time_dict['target_time']
            else:
                time_dict = cls.get_target_time(pattern, source_info.file_proto_info.source_file_nm, start_time, target_time)
                source_info.file_proto_info.source_file_nm = time_dict['change_nm']
                if source_info.file_proto_info.target_time is None:
                    source_info.file_proto_info.target_time = time_dict['target_time']

        def _apply_time_pattern_to_interface(is_dir_pattern, pattern, interface_info, target_time, start_time):
            if is_dir_pattern:
                time_dict = cls.get_target_time(pattern, interface_info.ftp_proto_info.source_dir, start_time, target_time)
                interface_info.ftp_proto_info.source_dir = time_dict['change_nm']
            else:
                time_dict = cls.get_target_time(pattern, interface_info.ftp_proto_info.source_file_nm, start_time, target_time)
                interface_info.ftp_proto_info.source_file_nm = time_dict['change_nm']

            logger.info(f"time_dict : {time_dict}")

            interface_info.target_time = time_dict['target_time']
            logger.info(f"changed interface_info : {interface_info}")
            for source_info in source_infos:
                if source_info.file_proto_info.target_time is None:
                    source_info.file_proto_info.target_time = time_dict['target_time']

        logger.info("changing source_info time pattern")
        for source_info in source_infos:
            file_source_pattern = cls.get_time_pattern(source_info.file_proto_info.source_file_nm)
            file_nm_pattern = cls.get_time_pattern(source_info.file_proto_info.file_nm)

            if file_nm_pattern:
                _apply_time_pattern_to_source_info(file_nm_pattern, None, source_info, interface_info.start_time, target_time)
            if file_source_pattern:
                _apply_time_pattern_to_source_info(None, file_source_pattern, source_info, interface_info.start_time, target_time)

        logger.info("changing interface time pattern")
        interface_filenm_pattern = cls.get_time_pattern(interface_info.ftp_proto_info.source_file_nm)
        source_dir_timepattern = cls.get_time_pattern(interface_info.ftp_proto_info.source_dir)

        if interface_filenm_pattern or source_dir_timepattern:
            if target_time:
                if interface_filenm_pattern:
                    _apply_time_pattern_to_interface(False, interface_filenm_pattern, interface_info, target_time, interface_info.start_time)
                if source_dir_timepattern:
                    _apply_time_pattern_to_interface(True, source_dir_timepattern, interface_info, target_time, interface_info.start_time)
            else:
                if interface_filenm_pattern:
                    _apply_time_pattern_to_interface(False, interface_filenm_pattern, interface_info, None, interface_info.start_time)
                if source_dir_timepattern:
                    _apply_time_pattern_to_interface(True, source_dir_timepattern, interface_info, None, interface_info.start_time)

        logger.info("interface_info.file_nm: %s", interface_info.ftp_proto_info.source_file_nm)

        if not any(source_info.file_proto_info.target_time for source_info in source_infos):
            target_time = current_time.strftime("%Y%m%d%H%M")
            for source_info in source_infos:
                source_info.file_proto_info.target_time = target_time
            if interface_info.target_time is None:
                interface_info.target_time = target_time

    @classmethod
    def get_time_value(cls, time_pattern, time_value, operator_str):
        time_value = datetime.strptime(time_value, "%Y%m%d%H%M")

        operator_func = cls._get_operator_func(operator_str)

        logger.debug("getTimeValue timePatterns: %s", time_pattern)

        num, char = cls._extract_number_and_char(time_pattern)
        time_value = cls._apply_time_change(time_value, num, char, operator_func)

        logger.debug("getTimeValue calculated timeValue: %s", time_value)

        result_time = cls._format_time_value(time_value, time_pattern)

        if result_time is None:
            logger.exception("result_time is None")

        return result_time

    @staticmethod
    def _get_operator_func(operator_str):
        operator_mapping = {
            "+": operator.add,
            "-": operator.sub
        }

        operator_func = operator_mapping.get(operator_str)
        if operator_func is None:
            raise ValueError(f"Invalid operator: {operator_str}")
        return operator_func

    @staticmethod
    def _extract_number_and_char(pattern):
        num = int(re.search(r'\d+', pattern).group())
        char = re.search(r'[a-zA-Z]', pattern).group()
        return num, char

    @staticmethod
    def _apply_time_change(time_value, num, char, operator_func):
        time_unit_map = {
            "y": relativedelta(years=num),
            "M": relativedelta(months=num),
            "d": timedelta(days=num),
            "H": timedelta(hours=num),
            "m": timedelta(minutes=num)
        }

        if char not in time_unit_map:
            raise ValueError(f"Invalid time character: {char}")

        return operator_func(time_value, time_unit_map[char])

    @staticmethod
    def _format_time_value(time_value, time_patterns):
        if isinstance(time_patterns, list):
            time_patterns = time_patterns[0]

        format_map = {
            'y': "%Y",
            'M': "%Y%m",
            'd': "%Y%m%d",
            'H': "%Y%m%d%H",
            'm': "%Y%m%d%H%M"
        }
        last_char = time_patterns[-1]

        if last_char not in format_map:
            raise ValueError(f"Invalid time character: {last_char}")

        return time_value.strftime(format_map.get(last_char, ""))

    @classmethod
    def check_pattern(cls, param):
        match = re.search(r'\d+\w', param)
        if match:
            return match.group(0)
        else:
            return None

    @classmethod
    def get_sql_params(cls, query_params, current_time):
        return [cls._resolve_sql_param(param, current_time) for param in query_params]

    @classmethod
    def _resolve_sql_param(cls, param, current_time):
        if not cls._is_time_pattern(param):
            logger.warning("No time pattern found in param: %s", param)
            return param

        logger.info("Processing time pattern param: %s", param)

        if cls._is_calculated_pattern(param):
            return cls._resolve_calculated_param(param, current_time)
        else:
            return cls._resolve_simple_time_param(param, current_time)

    @classmethod
    def _resolve_calculated_param(cls, param, current_time):
        pattern = cls.check_pattern(param)
        operator = "-" if "-" in param else "+"
        result = cls.get_time_value(pattern, current_time, operator)
        logger.info("Calculated time: %s", result)
        return result

    @classmethod
    def _resolve_simple_time_param(cls, param, current_time):
        dt = datetime.strptime(current_time, "%Y%m%d%H%M")
        result = cls._format_time_pattern(param, dt)
        logger.info("Formatted time: %s", result)
        return result

    @staticmethod
    def _is_time_pattern(param):
        return "${" in param

    @staticmethod
    def _is_calculated_pattern(param):
        return "-" in param or "+" in param

    @classmethod
    def get_query_using_row_num_data_query(cls, data_query, start_row_num, end_row_num, max_row_count):
        if end_row_num == -1:
            end_row_num = max_row_count
        params = ["#", str(end_row_num), str(start_row_num), str(end_row_num), str(end_row_num)]

        if len(params) != data_query.count('#'):
            logger.error("data_query formatting fail len(params): %s, count('#'): %s", len(params),
                         data_query.count('#'))

        formatted_sql = data_query.replace('#', '{}')
        formatted_sql = formatted_sql.format(*params)

        logger.debug("getQueryUsingRowNum params: %s", params)
        logger.debug("getQueryUsingRowNumDataQuery SQL: %s", formatted_sql)

        return formatted_sql

    @classmethod
    def get_query_using_row_num(cls, data_query, start_row_num, end_row_num, db_type):
        select_index = data_query.lower().index("select")

        if db_type == "ORACLE":
            sub_query = "SELECT ROWNUM AS rn," + data_query[select_index + len("select"):]
        else:
            sub_query = "SELECT ROW_NUMBER() OVER () AS rn," + data_query[select_index + len("select"):]

        params = [sub_query, start_row_num]
        sql = "SELECT * FROM ({}) subquery WHERE rn > {} AND rn <= {}"

        if end_row_num == -1:
            sql = "SELECT * FROM ({}) subquery WHERE rn > {}"
        else:
            params.append(end_row_num)

        formatted_sql = sql.format(*params)
        logger.info("getQueryUsingRowNum SQL: %s", formatted_sql)

        return formatted_sql

    @classmethod
    def get_row_num_list(cls, record_count, num_rows):
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

    @classmethod
    def get_data_query_list(cls, row_num_list, num_rows, data_query, record_count, db_proto_info):
        # according to num_rows, query_list size can be 1 or n
        query_list = []
        if num_rows > 0:
            for index in range(len(row_num_list)):
                start_row_num = row_num_list[index]

                if index == len(row_num_list) - 1:
                    end_row_num = -1
                else:
                    end_row_num = row_num_list[index + 1]

                if "ROWNUM" in data_query:
                    query = cls.get_query_using_row_num_data_query(data_query, start_row_num, end_row_num, record_count)
                else:
                    query = cls.get_query_using_row_num(data_query, start_row_num, end_row_num, db_proto_info.type)
                query_list.append(query)
        else:
            query_list.append(data_query)

        return query_list

    # latest date is targetTime
    @classmethod
    def get_oracle_target_time(cls, param_dic):
        target_time = param_dic[0]

        for item in param_dic:
            if item < target_time:
                target_time = item

        logger.info("partitions targetDT %s", target_time)

        return target_time

    @classmethod
    def get_clean_filename(cls, file_path: str) -> str:
        return cls.strip_date_in_filename(cls.remove_all_suffixes(os.path.basename(file_path)))

    @classmethod
    def remove_all_suffixes(cls, file_name: str) -> str:
        cleaned_name = file_name
        while Path(cleaned_name).suffixes:
            cleaned_name = Path(cleaned_name).stem
        return cleaned_name

    @classmethod
    def strip_date_in_filename(cls, file_name, date_pattern=r"(?:_|-)?\d{6,}(?:\d{2})?(?:[-_]?\d{4})?(?:_|-)?"):
        # abc20240115, abc-20250115, abc_20250115, 20250115abc -> abc
        return re.sub(date_pattern, "", str(file_name))

    @classmethod
    def format_query_for_log(cls, sql, params):
        placeholders = tuple(["'" + str(p) + "'" if isinstance(p, str) else str(p) for p in params])
        return sql % placeholders

    @classmethod
    def get_file_nm(cls, src_dt, target_file):
        # time pattern ${yyyyMMddHH}
        pattern = r'\${.*?}|\\d{(6|8|10|12)}'
        match = re.search(pattern, target_file)

        if match:
            dt_pattern = match.group()
            formatted_dt = cls._format_datetime(dt_pattern, src_dt)
            return re.sub(pattern, formatted_dt, target_file)

        return target_file

    @classmethod
    def get_file_nm_by_pattern(cls, src_dt, target_file):
        time_pattern = r'\${[a-zA-Z0-9_\-]+}'
        match_patterns = re.findall(time_pattern, target_file)

        for pattern in match_patterns:
            clean_cal_pattern = re.sub(r'-\d+[dMhH]', '', pattern)

            dt_value = cls._format_datetime_by_pattern(src_dt, clean_cal_pattern)
            target_file = target_file.replace(pattern, dt_value)
        return target_file

    @classmethod
    def _format_datetime_by_pattern(cls, src_dt, pattern):
        time_pattern_mapping = {
            "${yyyy}": "%Y",
            "${MM}": "%m",
            "${dd}": "%d",
            "${HH}": "%H",
            "${yyyyMM}": "%Y%m",
            "${yyyyMMdd}": "%Y%m%d",
            "${yyyyMMddHH}": "%Y%m%d%H",
            "${yyyyMMdd_HH}": "%Y%m%d_%H",
            "${yyyyMMdd HH}": "%Y%m%d%H",
            "${yyyyMMdd_HHmm}": "%Y%m%d_%H%M",
            "${yyyyMMdd HHmm}": "%Y%m%d%H%M",
            "${yyyyMMddHHmm}": "%Y%m%d%H%M",
            "${yyyyMM}01": "%Y%m01",
            "${yyMMddHH}": "%y%m%d%H",
            "${HHmm}": "%H%M"
        }
        date_format_mapping = {
            12: "%Y%m%d%H%M",
            10: "%Y%m%d%H",
            8: "%Y%m%d",
            6: "%Y%m",
            4: "%Y",
        }
        dt = datetime.strptime(src_dt, date_format_mapping[len(src_dt)])
        return dt.strftime(time_pattern_mapping[pattern])
    @staticmethod
    def _format_datetime(dt_pattern, src_dt):
        pattern_mapping = {
            'yyyyMMddhhmm': 12,  # 'yyyyMMddhhmm' -> 12 characters
            'yyyyMMddHH': 10,
            'yyyyMMddhh': 10,
            'yyyyMMdd': 8,
            'yyyyMM': 6,
            'yyMMddHH': 8,
            '\\d{12}': 12,  # '\\d{12}' -> 12 characters
            '\\d{10}': 10,
            '\\d{8}': 8,
            '\\d{6}': 6,
        }
        logger.info(f"dt_pattern: {dt_pattern}, src_dt:{src_dt}")
        for pattern_key, length in pattern_mapping.items():
            if pattern_key in dt_pattern:
                return src_dt[:length]

        return ""  # Default return if no matching pattern is found

def run_test_cases():
    test_cases = [
        ("/path/dir1/dir2/abc_20250120.tar.gz", "abc"),
        ("/path/dir1/dir2/abc-20250120.tar.gz", "abc"),
        ("/path/dir1/dir2/20250120_abc.tar.gz", "abc"),
        ("/path/dir1/dir2/20250120-abc.tar.gz", "abc"),
        ("/path/dir1/dir2/20250120104021-abc.tar.gz", "abc"),
        ("/path/dir1/dir2/20250120104021_abc.tar.gz", "abc"),
        ("/path/dir1/dir2/20250120104021_abc.csv", "abc"),
        ("/path/dir1/dir2/20250120104021-abc.csv", "abc"),
        ("/path/dir1/dir2/abc-20250120104021.csv", "abc"),
        ("/path/dir1/dir2/abc_20250120104021.csv", "abc"),
    ]

    for file_path, expected in test_cases:
        result = Util.get_clean_filename(file_path)
        assert result == expected, (
            f"Test failed for file_path: {file_path}. "
            f"Expected: {expected}, Got: {result}"
        )
    print("All tests passed!")


def test_get_time_value():
    test_cases = [
        ('1y', '202301010000', '+', '2024'),
        ('1M', '202301010000', '-', '202212'),
        ('10d', '202301010000', '+', '20230111'),
        ('1H', '202301010000', '-', '2022123123'),
        ('30m', '202301010000', '+', '202301010030'),
        ('2y', '202301010000', '+', '2025'),
        ('1y', '202301010000', '+', '2024'),
        ('2M', '202301010000', '+', '202303'),
        ('1d', '202301010000', '-', '20221231'),
        ('12H', '202301010000', '+', '2023010112'),
        ('90m', '202301010000', '+', '202301010130'),
        ('1M', '202301312300', '+', '202302'),
        ('1y', '202301010000', '-', '2022'),
    ]

    for idx, (time_pattern, time_value, operator, expected) in enumerate(test_cases):
        result = Util.get_time_value(time_pattern, time_value, operator)
        assert result == expected, f"Test case {idx + 1} failed: Expected {expected}, got {result}"

    # 예외 케이스
    exception_cases = [
        ('1Q', '202301010000', '+'),          # 잘못된 단위
        ('', '202301010000', '+'),            # 빈 패턴
        ('1d', '', '+'),                      # 빈 날짜
        ('1d', '2023-01-01', '+'),            # 잘못된 날짜 포맷
        ('1d', '202301010000', '*'),          # 잘못된 연산자
    ]

    for idx, (time_pattern, time_value, operator) in enumerate(exception_cases, start=len(test_cases)+1):
        try:
            Util.get_time_value(time_pattern, time_value, operator)
            assert False, f"Test case {idx} failed: Expected exception but got success"
        except Exception as e:
            print(f"Test case {idx} passed (expected exception): {e}")

    print("All tests passed!")


def test_invalid_operator():
    try:
        Util.get_time_value(['1d'], '202301010000', '*')  # 잘못된 연산자
    except ValueError as e:
        print(f"Caught expected ValueError for invalid operator: {e}")
    else:
        print("Failed to raise ValueError for invalid operator")


def test_format_time_value():
    test_cases = [
        # (time_value, time_pattern, expected_result)
        (datetime(2023, 1, 1, 12, 30), "1y", "2023"),
        (datetime(2023, 1, 1, 12, 30), "1M", "202301"),
        (datetime(2023, 1, 1, 12, 30), "1d", "20230101"),
        (datetime(2023, 1, 1, 12, 30), "1H", "2023010112"),
        (datetime(2023, 1, 1, 12, 30), "1m", "202301011230"),

        # 다양한 날짜
        (datetime(2021, 5, 15, 18, 45), "1y", "2021"),
        (datetime(2021, 5, 15, 18, 45), "1M", "202105"),
        (datetime(2021, 5, 15, 18, 45), "1d", "20210515"),
        (datetime(2021, 5, 15, 18, 45), "1H", "2021051518"),
        (datetime(2021, 5, 15, 18, 45), "1m", "202105151845"),

        # 예외 케이스 (잘못된 패턴)
        (datetime(2023, 1, 1, 12, 30), "x", ValueError),
    ]

    for idx, (time_value, time_pattern, expected) in enumerate(test_cases):
        if isinstance(expected, type) and issubclass(expected, Exception):
            try:
                Util._format_time_value(time_value, time_pattern)
                assert False, f"Test case {idx + 1} failed: Expected exception {expected} not raised"
            except ValueError as e:
                pass
            except Exception as e:
                assert False, f"Test case {idx + 1} failed: Unexpected exception {type(e)}: {str(e)}"
        else:
            result = Util._format_time_value(time_value, time_pattern)
            assert result == expected, f"Test case {idx + 1} failed: Expected {expected}, got {result}"

    print("All valid test cases passed!")


def test_get_file_nm():
    test_cases = [
        ("2025053015", "DTS_${yyyyMMdd}.dat", "DTS_20250530.dat"),
        ("202505302130", "prity_mode_scrbr_cnt_1h_${yyyyMMdd}", "prity_mode_scrbr_cnt_1h_20250530"),
        ("202505301530", "tmap_poimeta_category_${yyyyMMdd}.dat", "tmap_poimeta_category_20250530.dat"),
        ("20250530", "${yyyyMMdd}0000_ai_smap_anal_5g_.dat", "202505300000_ai_smap_anal_5g_.dat"),
        ("202505301530", "per_lncel_1h_${yyyyMMddhh}.csv", "per_lncel_1h_2025053015.csv"),
        ("202505301530", "mdt_elg_raw_1d_${yyyyMMddHH}.dat", "mdt_elg_raw_1d_2025053015.dat"),
        ("202505301530", "raccu_15m_${yyyyMMddhhmm}.csv", "raccu_15m_202505301530.csv"),
        ("2025041613", "dpsx_5g_all_pgw_kpi_5m_\\d{10}.csv", "dpsx_5g_all_pgw_kpi_5m_2025041613.csv"),
        ("202505301530", "volume_pg_\\d{10}.dat", "volume_pg_2025053015.dat"),
        ("202505301530", "nsn_anomaly_1d_v4_\d{10}.csv", "nsn_anomaly_1d_v4_2025053015.csv"),
        ("202505301530", "10days_1d_w_\d{10}.csv", "10days_1d_w_2025053015.csv"),
        ("20250529", "traffic_1h_\d{8}.dat", "traffic_1h_20250529.dat"),
        ("202505301530", "kpi_15m_\d{12}.csv", "kpi_15m_202505301530.csv")
    ]


    for idx, (src_dt, target_file, expected) in enumerate(test_cases):
        if isinstance(expected, type) and issubclass(expected, Exception):
            try:
                Util.get_file_nm(src_dt, target_file)
                assert False, f"Test case {idx + 1} failed: Expected exception {expected} not raised"
            except ValueError as e:
                pass
            except Exception as e:
                assert False, f"Test case {idx + 1} failed: Unexpected exception {type(e)}: {str(e)}"
        else:
            result = Util.get_file_nm(src_dt, target_file)
            assert result == expected, f"Test case {idx + 1} failed: Expected {expected}, got {result}"

    print("All valid test cases passed!")


def test_get_calculate_pattern():
    test_cases = [
        # (pattern, started_time, expected_result)
        ("${yyyyMMdd+1d}", "202301010000", "20230102"),  # 1일 더하기
        ("${yyyyMMddHH+1H}", "202301010000", "2023010101"),  # 1시간 더하기
        ("${yyyyMMddHH-1H}", "202301010000", "2022123123"),  # 1시간 빼기
        ("${yyyyMMddHH+30m}", "202301010000", "2023010100"),  # 30분 더하기
        ("${yyyyMMddHH-30m}", "202301010000", "2022123123"),  # 30분 빼기
        ("${yyyyMMdd+2M}", "202301010000", "20230301"),  # 2개월 더하기
        ("${yyyyMMdd-2M}", "202301010000", "20221101"),  # 2개월 빼기
        ("${yyyyMMdd+1M}", "202301010000", "20230201"),  # 1개월 더하기
        ("${yyyyMMdd-1M}", "202301010000", "20221201"),  # 1개월 빼기
        ("${yyyyMMdd+1y}", "202301010000", "20240101"),  # 1년 더하기
        ("${yyyyMMdd+1d}", "202301010000", "20230102"),  # 1일 더하기
        ("${yyyyMMdd-1d}", "202301010000", "20221231"),  # 1일 빼기
        ("", "202301010000", ValueError),
        ("+1y", "202301010000", ValueError),
    ]

    for idx, (pattern, started_time, expected) in enumerate(test_cases):
        if isinstance(expected, type) and issubclass(expected, Exception):
            try:
                Util.get_calculate_pattern(pattern, started_time)
                assert False, f"Test case {idx + 1} failed: Expected exception {expected} not raised"
            except ValueError as e:
                pass
            except Exception as e:
                assert False, f"Test case {idx + 1} failed: Unexpected exception {type(e)}: {str(e)}"
        else:
            result = Util.get_calculate_pattern(pattern, started_time)
            assert result == expected, f"Test case {idx + 1} failed: Expected {expected}, got {result}"

    print("All valid test cases passed!")


def test_get_target_time():
    test_cases = [
        # (time_patterns, file_info, start_time, target_time, expected_result)
        # 날짜만 포함된 파일 이름 패턴 처리
        (["${yyyyMM}"], "data_${yyyyMM}.txt", "202301010000", None, {'change_nm': 'data_202301.txt', 'target_time': '202301010000'}),
        (["${yyyyMMdd}"], "data_${yyyyMMdd}.txt", "202301010000", None, {'change_nm': 'data_20230101.txt', 'target_time': '202301010000'}),

        # 날짜 오프셋 테스트 (월말 계산)
        (["${yyyyMMdd+1d}"], "log_${yyyyMMdd+1d}.log", "202301312359", None, {'change_nm': 'log_20230201.log', 'target_time': '202302012359'}),  # 월말 계산
        (["${yyyyMMdd-1d}"], "file_${yyyyMMdd-1d}.txt", "202301010000", None, {'change_nm': 'file_20221231.txt', 'target_time': '202212310000'}),  # 1일 빼기

        # 날짜 오프셋 정상 계산
        (["${yyyyMMdd+1d}"], "data_${yyyyMMdd+1d}.txt", "202301010000", None, {'change_nm': 'data_20230102.txt', 'target_time': '202301020000'}),  # 1일 더하기
        (["${yyyyMMdd+1d}"], "report_${yyyyMMdd+1d}.xlsx", "202301010000", None, {'change_nm': 'report_20230102.xlsx', 'target_time': '202301020000'}),  # 정상 계산

        # 시간 오프셋 테스트
        (["${yyyyMMddHH+1H}"], "log_${yyyyMMddHH+1H}.log", "202301010000", None, {'change_nm': 'log_2023010101.log', 'target_time': '202301010100'}),  # 1시간 더하기
        (["${yyyyMMddHH-30m}"], "log_${yyyyMMddHH-30m}.log", "202301010000", None, {'change_nm': 'log_2022123123.log', 'target_time': '202212312330'}),  # 30분 빼기

        # 월, 연도 오프셋
        (["${yyyyMMdd+2M}"], "backup_${yyyyMMdd+2M}.zip", "202301010000", None, {'change_nm': 'backup_20230301.zip', 'target_time': '202303010000'}),  # 2개월 더하기
        (["${yyyyMMdd-1M}"], "data_${yyyyMMdd-1M}.csv", "202301010000", None, {'change_nm': 'data_20221201.csv', 'target_time': '202212010000'}),  # 1개월 빼기

        # target_time이 주어졌을 경우 (기존 날짜 계산된 것과 다르게 처리)
        (["${yyyyMMdd+1d}"], "report_${yyyyMMdd+1d}.xlsx", "202301010000", "202302010000", {'change_nm': 'report_20230201.xlsx', 'target_time': '202302010000'}),  # 1일 더하기
        (["${yyyyMMddHH+1H}"], "report_${yyyyMMddHH+1H}.xlsx", "202301010000", "202302010000", {'change_nm': 'report_2023020100.xlsx', 'target_time': '202302010000'}),  # 1시간 더하기

        # 범위 벗어나는 날짜 처리 (2월 28일, 2월 29일)
        (["${yyyyMMdd+1d}"], "log_20230228.log", "202302282359", None, {'change_nm': 'log_20230228.log', 'target_time': '202303012359'}),  # 2월 28일 +1일
        (["${yyyyMMdd+1d}"], "log_20230229.log", "202302292359", None, ValueError),  # 2월 29일 +1일 (윤년) 해당 테스트는 ValueError: day is out of range for month 발생

        # 다양한 날짜/시간 조합
        (["${yyyyMMddHH+1H}"], "log_${yyyyMMddHH+1H}.log", "202312312359", None, {'change_nm': 'log_2024010100.log', 'target_time': '202401010059'}),  # 1시간 더하기 - 연말 계산
        (["${yyyyMMdd+1d}"], "backup_${yyyyMMdd+1d}.zip", "202301310000", None, {'change_nm': 'backup_20230201.zip', 'target_time': '202302010000'}),  # 월말 계산

        # 여러 패턴이 포함된 경우 - 날짜 계산 후 결과 리스트로 반환
        (["${yyyyMMdd+1d}", "${yyyyMMdd+1d}"], "data_${yyyyMMdd+1d}.txt", "202301010000", None, {'change_nm': 'data_20230102.txt', 'target_time': '202301020000'}),  # 1일 더하고, 월을 추가

        # 매핑 할 날짜 패턴이 여러 개인 경우
        (["${yyyyMMdd-1H}", "${HH-1H}"], "data_${yyyyMMdd-1H}_${HH-1H}.txt", "202401012200", None, {'change_nm': 'data_20240101_21.txt', 'target_time': '202401012100'}),

        # 패턴이 없을 때
        (["${non_existing_pattern}"], "data_${yyyyMMdd}.txt", "202301010000", None, ValueError),  # 패턴이 없을 때

        # 잘못된 날짜 형식 처리
        (["${yyyyMMdd}"], "log_${yyyyMMdd}.log", "invalid_start_time", None, ValueError),  # 잘못된 날짜 형식

        (["\\d{10}"], "dpsx_5g_all_pgw_kpi_5m_\\d{10}.csv", "202504161300", None,
         {'change_nm': 'dpsx_5g_all_pgw_kpi_5m_2025041613.csv', 'target_time': '202504161300'}),

        (["\\d{12}"], "kisa_gw_traffic_\\d{12}.dat", "202504161300", None,
         {'change_nm': 'kisa_gw_traffic_202504161300.dat', 'target_time': '202504161300'}),

        (["\\d{6}"], "imsi_user_cnt_1m_\\d{6}.dat", "202504161300", None,
         {'change_nm': 'imsi_user_cnt_1m_202504.dat', 'target_time': '202504161300'}),

        # 잘못된 정규표현식 처리
        (["\\d{5}"], "invalid_file_\\d{5}.txt", "202504161300", None, ValueError),
    ]

    for idx, (time_patterns, file_info, start_time, target_time, expected) in enumerate(test_cases):
        if isinstance(expected, type) and issubclass(expected, Exception):
            try:
                Util.get_target_time(time_patterns, file_info, start_time, target_time)
                assert False, f"Test case {idx + 1} failed: Expected exception {expected} not raised"
            except ValueError as e:
                pass
            except Exception as e:
                assert False, f"Test case {idx + 1} failed: Unexpected exception {type(e)}: {str(e)}"
        else:
            result = Util.get_target_time(time_patterns, file_info, start_time, target_time)
            assert result == expected, f"Test case {idx + 1} failed: Expected {expected}, got {result}"

    print("All test cases passed!")


def test_distcp_ftp_provide_path_time_pattern_changer():
    src_dts = ["202510231130", "2025102311", "20251023","202510","2025"]
    target_file = ["/test/dt=${yyyyMMdd}", "/test/dt=${yyyyMMdd}/hh={HH}", "/test/dt=${yyyyMMdd}/hm=${HHmm}", "/test/event_time=${yyyyMMdd}/v3/${HH}",
                   "/test/v3/${HHmm}", "/test/dt=${yyyyMMdd-1d}", "/test/dt=${yyyyMMdd-1h}", "/test/dt=${yyyyMMdd-1M}", "/test/dt=${yyyyMMdd-1m}"]


if __name__ == '__main__':
    run_test_cases()
    test_get_time_value()
    test_invalid_operator()
    test_format_time_value()
    test_get_calculate_pattern()
    test_get_target_time()
    test_get_file_nm()
