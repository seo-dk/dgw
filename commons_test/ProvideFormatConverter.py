import logging
import os.path
import re

from commons_test.ProvideMetaInfoHook import ProvideInterface
from commons_test.MetaInfoHook import SourceInfo, InterfaceInfo
from commons_test.SparkSubmitLauncher import SparksubmitLauncher, ParquetConversionConfig

from airflow.models import Variable

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))


class ProvideFormatConverter:
    """Provide 시 Spark를 사용한 포맷 변환 클래스
    
    이 클래스는 데이터 제공(Provide) 단계에서 사용되는 포맷 변환을 처리합니다.
    Collect 단계가 아닌 Provide 단계에서만 사용됩니다.
    """
    
    def __init__(self, hdfs_src_path, provide_interface: ProvideInterface, collect_interface: InterfaceInfo):
        self.hdfs_src_path = hdfs_src_path
        self.provide_interface = provide_interface
        self.collect_interface = collect_interface
        self.target_format = provide_interface.proto_info_json.convert_to
        
        # 경로 설정
        self._prepare_paths()
    
    def _prepare_paths(self):
        """변환에 필요한 경로들을 준비"""
        # 소스 경로 설정
        if not self.hdfs_src_path.startswith("hdfs://"):
            self.hdfs_src_path_full = "hdfs://dataGWCluster" + self.hdfs_src_path
        else:
            self.hdfs_src_path_full = self.hdfs_src_path
        
        # 파일명이 포함되어 있는지 확인 (확장자가 있으면 파일로 간주)
        src_path_for_output = self.hdfs_src_path_full
        if os.path.splitext(src_path_for_output)[1]:
            # 파일명이 포함되어 있으면 디렉토리 경로만 사용
            src_path_for_output = os.path.dirname(src_path_for_output)
            logger.info("Filename detected in hdfs_src_path_full, using directory path: %s", src_path_for_output)
        
        # 출력 경로 설정 (staging 제거한 경로에 변환된 파일 저장)
        # ConvertToCsv와 동일하게 converted_{format} 서브디렉토리 사용하여 원본 파일과 겹치지 않도록 함
        self.hdfs_output_path = src_path_for_output.replace("/staging", "").replace("/idcube_out", f"/idcube_out/converted_{self.target_format}")
        
        logger.info("hdfs_src_path_full: %s, hdfs_output_path: %s", 
                   self.hdfs_src_path_full, self.hdfs_output_path)
        
    
    def execute_convert(self):
        """포맷 변환 실행"""
        if not self._should_convert():
            return None
        
        logger.info(f"{self.provide_interface.interface_id} need to convert to {self.target_format}")
        logger.info(f"hdfs_path : {self.hdfs_src_path}")
        
        # Spark 설정 생성
        spark_config = self._create_spark_config()
        
        # 변환 실행
        converted_path = self._execute_spark_conversion(spark_config)
        
        # 변환된 경로 반환 (hdfs:// prefix 제거)
        return self._normalize_path(converted_path)
    
    def _should_convert(self):
        """포맷 변환이 필요한지 확인"""
        if not self.target_format:
            return False
        
        # CSV 변환은 ConvertToCsv에서 처리하므로 여기서는 스킵
        if self.target_format == "csv":
            return False
        
        if self.provide_interface.proto_info_json.spark_server_info.is_external:
            return False
        
        if self.provide_interface.proto_info_json.use_raw_file:
            return False
        
        return True
    
    def _create_spark_config(self):
        """Spark 변환 설정 생성"""
        # 기본 설정값 가져오기
        delimiter = self.provide_interface.proto_info_json.delimiter
        source_format = self.provide_interface.proto_info_json.format
        target_compression = self.provide_interface.proto_info_json.target_compression
        source_compression = self.provide_interface.proto_info_json.source_compression
        header_skip = self.provide_interface.proto_info_json.header_skip
        
        # encoding 정보는 collect_interface에서 가져오기
        encoding = None
        if self.collect_interface.protocol_cd == 'DISTCP' and self.collect_interface.distcp_proto_info:
            encoding = self.collect_interface.distcp_proto_info.encode
        elif self.collect_interface.protocol_cd in ['FTP', 'SFTP'] and self.collect_interface.ftp_proto_info:
            encoding = self.collect_interface.ftp_proto_info.encode
        
        # hdfs_src_path에 파일명이 포함되어 있는지 확인
        logger.info(f"hdfs_src_path_full : {self.hdfs_src_path_full}")
        logger.info(f"hdfs_src_path : {self.hdfs_src_path}")

        merge_file_nm = self._check_need_merge()
        
        return ParquetConversionConfig(
            delimiter=delimiter,
            source_format=source_format,
            target_format=self.target_format,
            target_compression=target_compression,
            source_compression=source_compression,
            encoding=encoding,
            header_skip=header_skip,
            merge_to_one_file=self.provide_interface.proto_info_json.merge,
            merge_file_nm=merge_file_nm
        )

    def _check_need_merge(self):
        merge_file_nm = ""
        logger.info(f"hdfs_src_path_full : {self.hdfs_src_path_full}")
        if self.provide_interface.proto_info_json.merge:
            # 파일명이 포함되어 있는지 확인 (확장자가 있으면 파일로 간주)
            if os.path.splitext(self.hdfs_src_path_full)[1]:
                # 파일명이 포함되어 있으면 파일명에서 확장자를 제거하여 사용
                filename = os.path.basename(self.hdfs_src_path_full)
                merge_file_nm = os.path.splitext(filename)[0]  # 확장자 제거
                logger.info(f"Using filename from path as merge_file_nm: {merge_file_nm}")
            else:
                # 파일명이 없으면 기존 로직대로 파티션 정보에서 파일명 생성
                keys = ["tb", "dt", "hh", "hm"]
                values = []
                found_keys = set()  # 경로에서 찾은 키 추적
                
                for key in keys:
                    m = re.search(rf'{key}=([^/]+)',self.hdfs_src_path_full)
                    if m:
                        values.append(m.group(1))
                        found_keys.add(key)
                
                # interface_cycle에 따라 필요한 시간 파티션이 경로에 없으면 target_time에서 추출
                values = self._enrich_partition_values_with_missing_time(values, found_keys, keys)
                
                merge_file_nm = "_".join(values)
        logger.info(f"merge_file_nm : {merge_file_nm}")
        return merge_file_nm
    
    def _enrich_partition_values_with_missing_time(self, values, found_keys, keys):
        """경로에서 누락된 시간 파티션을 target_time에서 추출하여 values에 추가"""
        if not self.collect_interface or not self.collect_interface.interface_cycle:
            return values
        
        interface_cycle = self.collect_interface.interface_cycle
        time_partition_config = self._get_time_partition_config(interface_cycle)
        
        if not time_partition_config:
            return values
        
        partition_key, required_length, start_pos, end_pos = time_partition_config
        
        # 이미 파티션이 존재하면 추가하지 않음
        if partition_key in found_keys:
            return values
        
        # target_time에서 시간 값 추출
        time_value = self._extract_time_value_from_target_time(required_length, start_pos, end_pos)
        if not time_value:
            return values
        
        # dt 인덱스 찾기 및 적절한 위치에 삽입
        dt_index = self._find_dt_index_in_values(keys, found_keys)
        insert_position = dt_index + 1 if dt_index is not None else len(values)
        values.insert(insert_position, time_value)
        
        logger.info(f"Added {partition_key}={time_value} from target_time for {interface_cycle} cycle")
        return values
    
    def _get_time_partition_config(self, interface_cycle):
        """interface_cycle에 따른 시간 파티션 설정 반환
        
        Returns:
            tuple: (partition_key, required_length, start_pos, end_pos) 또는 None
        """
        config_map = {
            'HH': ('hh', 10, 8, 10),   # YYYYMMDDHH 형식에서 HH 추출
            'MI': ('hm', 12, 8, 12),   # YYYYMMDDHHMM 형식에서 HHMM 추출
        }
        return config_map.get(interface_cycle)
    
    def _extract_time_value_from_target_time(self, required_length, start_pos, end_pos):
        """target_time에서 지정된 위치의 시간 값 추출
        
        Args:
            required_length: target_time의 최소 길이
            start_pos: 추출 시작 위치
            end_pos: 추출 종료 위치
            
        Returns:
            str: 추출된 시간 값 또는 None
        """
        if not self.collect_interface.target_time:
            return None
        
        target_time = self.collect_interface.target_time
        if len(target_time) < required_length:
            return None
        
        return target_time[start_pos:end_pos]
    
    def _find_dt_index_in_values(self, keys, found_keys):
        """values 리스트에서 dt의 인덱스 위치 찾기
        
        Args:
            keys: 전체 키 순서 리스트 (예: ["tb", "dt", "hh", "hm"])
            found_keys: 경로에서 찾은 키들의 집합
            
        Returns:
            int: dt의 인덱스 위치 또는 None
        """
        if 'dt' not in found_keys:
            return None
        
        # dt 이전의 키들 중에서 찾은 것들의 개수가 dt의 인덱스
        dt_key_index = keys.index('dt')
        dt_index = sum(1 for k in keys[:dt_key_index] if k in found_keys)
        return dt_index


    def _execute_spark_conversion(self, spark_config):
        """Spark를 사용한 변환 실행"""
        spark_server_info = self.provide_interface.proto_info_json.spark_server_info
        
        # tb_name 추출
        tb_match = re.search(r'tb=([^/]+)', self.hdfs_src_path)
        tb_name = tb_match.group(1) if tb_match else None
        
        # memory_usage 가져오기
        memory_usage = spark_server_info.memory_usage if hasattr(spark_server_info, 'memory_usage') else "25g"
        
        # SparkSubmitLauncher로 변환 실행
        with SparksubmitLauncher(self.hdfs_src_path_full, self.hdfs_output_path, memory_usage,
                                 tb_name, spark_server_info, "parquet_conversion_test.py") as spark_launcher:
            converted_path = spark_launcher.start(spark_config)
            logger.info(f"converted_path : {converted_path}")
        
        return converted_path
    
    def _normalize_path(self, converted_path):
        """변환된 경로를 정규화 (hdfs:// prefix 제거)"""
        # Spark가 이미 /idcube_out/converted_{format} 경로에 저장했으므로, 원본과 겹치지 않음
        if converted_path.startswith("hdfs://dataGWCluster/"):
            return converted_path.replace("hdfs://dataGWCluster", "", 1)
        elif converted_path.startswith("hdfs://"):
            # 다른 hdfs:// prefix가 있는 경우 전체 prefix 제거
            # hdfs://host:port/path -> /path
            parts = converted_path.split("/", 3)
            if len(parts) >= 4:
                return "/" + parts[3]
            else:
                return converted_path
        else:
            return converted_path

