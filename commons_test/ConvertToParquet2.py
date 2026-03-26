import logging
import os
import time
import warnings
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import glob
import json
import re

import tempfile
from unittest.mock import patch, MagicMock
from collections import namedtuple
from decimal import Decimal
from billiard import Pool, cpu_count
from pyarrow.fs import HadoopFileSystem
from concurrent.futures import ThreadPoolExecutor
import functools

from commons_test.InfodatManager import InfoDatManager
from commons_test.MetaInfoHook import InterfaceInfo, SourceInfo
from commons_test.SchemaInfoManager import SchemaInfoManager
from commons_test.KafkaSend import KafkaSend
from commons_test.ParquetInfoManager import ParquetInfoManager, ParquetMessageInfo
from commons_test.CollectHistory import CollectHistory
from commons_test.CompressionManager import CompressionManager
from commons_test.PathUtil import PathUtil

logger = logging.getLogger(__name__)

warnings.simplefilter(action='ignore', category=FutureWarning)


def _safe_convert(convert_func):
    def wrapper(x):
        if x is None:
            return None
        try:
            return convert_func(x)
        except ValueError:
            return None

    return wrapper


def process_bigint_value(value):
    try:
        return int(float(value.strip()))
    except (ValueError, TypeError):
        return None


def get_converters(schema_dict):
    converters = {}
    for field in schema_dict['fields']:
        if field['type'] == 'string':
            converters[field['name']] = _safe_convert(str)
        elif field['type'] == 'int':
            converters[field['name']] = _safe_convert(lambda x: int(x.strip()))
        elif field['type'] == 'bigint':
            converters[field['name']] = _safe_convert(lambda x: process_bigint_value(x))
        elif field['type'] in ['float', 'double']:
            converters[field['name']] = _safe_convert(lambda x: float(x.strip()))
        elif field['type'] == 'boolean':
            converters[field['name']] = _safe_convert(lambda x: x.lower() in ['true', '1', 't', 'y', 'yes'])
        elif field['type'] == 'timestamp':
            converters[field['name']] = _safe_convert(lambda x: pd.to_datetime(x, format='%Y-%m-%d %H:%M:%S.%f', errors='coerce'))
        elif field['type'] == 'decimal':
            converters[field['name']] = _safe_convert(lambda x: Decimal(x.strip()))
        else:
            raise ValueError(f"Unsupported JSON type: {field['type']}")
    return converters


def convert(csv_file_pattern, source_info: SourceInfo, interface_info: InterfaceInfo, collect_history: CollectHistory, info_dat_path):
    csv_files = glob.glob(csv_file_pattern)
    if not csv_files:
        logger.warning("No CSV files found matching the pattern.")
        return

    total_size = sum(os.path.getsize(file) for file in csv_files)
    file_count = len(csv_files)

    average_size_mb = (total_size / file_count) / (1024 * 1024)

    logger.info(f"Average file size: {average_size_mb:.2f} MB")

    if average_size_mb >= 10:
        logger.info("Using parallel processing due to large file sizes.")
        convert_files(source_info, interface_info, collect_history, info_dat_path, csv_file_pattern, True)
    else:
        logger.info("Using sequential processing due to smaller file sizes.")
        convert_files(source_info, interface_info, collect_history, info_dat_path, csv_file_pattern)


def convert_files(source_info: SourceInfo, interface_info: InterfaceInfo, collect_history: CollectHistory, info_dat_path, csv_file_pattern, parallel=False):
    local_files = _decompress_gz_file(glob.glob(csv_file_pattern))

    logger.info("local_files before convert : %s", local_files)

    if not local_files:
        logger.info(f"No files matching pattern {csv_file_pattern} found.")

    try:
        schema_dict = SchemaInfoManager(source_info.hdfs_dir).get_schema_dict()
        if parallel:
            logger.info("working as parallel")
            with Pool(processes=get_worker_num(source_info)) as pool:
                pool.starmap(_convert, [(local_file, source_info, interface_info, schema_dict) for local_file in local_files], chunksize=1)
                pool.close()
                pool.join()
        else:
            local_files_parquet_msgs = _convert(local_files, source_info, interface_info, schema_dict)
            if info_dat_path:
                _send_message(local_files_parquet_msgs, source_info, interface_info, collect_history, info_dat_path)
    except Exception as e:
        logger.exception("Error in conversion")
        raise e


def _convert(local_files, source_info: SourceInfo, interface_info: InterfaceInfo, schema_dict, parquet_size_mb=2000):
    converters = get_converters(schema_dict)
    compression, delimiter, encode, merge = _initialize_conversion_params(interface_info, source_info)

    logger.info('[%s] started to convert', local_files)
    try:
        start_time = time.time()

        if isinstance(local_files, str):
            local_files = [local_files]

        base_name = os.path.splitext(os.path.basename(local_files[0]))[0]
        writer = None

        local_files_parquet_msgs = {}
        for local_file in local_files:
            if merge:
                match = re.search(r'tb=(.*?)/', local_file)
                if match:
                    base_name = match.group(1)
            writer, local_parquet_files = _convert_and_write(base_name, local_file, parquet_size_mb, writer, converters, delimiter, compression, schema_dict, encode)
            local_files_parquet_msgs[local_file] = local_parquet_files
        if writer is not None:
            writer.close()

        end_time = time.time()
        execution_time = end_time - start_time
        logger.info('Files converted to Parquet with approximately %s MB per file', parquet_size_mb)
        logger.info('%s Execution time: %.2f secs', local_files, execution_time)
        return local_files_parquet_msgs
    except Exception as e:
        logger.exception("Error converting %s", local_files, e)
        raise Exception(f"Failed to convert {local_files}") from e


def _convert_and_write(base_name, local_file, parquet_size_mb, writer, converters, delimiter, compression, schema_dict, encoding):
    local_parquet_files = []
    chunk_size = _get_chunk_size_based_on_file_size(local_file)
    current_file_index = 0
    current_size = 0
    schema = get_schema(schema_dict)
    field_names = get_field_names(schema_dict)
    csv_reader = pd.read_csv(local_file, header=None, names=field_names,
                             converters=converters, sep=delimiter, encoding=encoding,
                             na_values=['', ' '], engine='python', chunksize=chunk_size)

    parquet_file = local_file.replace("/idcube_out", "/idcube_out/converted")
    parquet_dir = os.path.dirname(parquet_file)

    if not os.path.exists(parquet_dir):
        os.makedirs(parquet_dir)

    for chunk in csv_reader:
        if chunk.empty:
            break

        if writer is None or current_size >= parquet_size_mb * 1024 * 1024:
            if writer is not None:
                writer.close()

            parquet_file = os.path.join(parquet_dir,
                                        f"{base_name}_{current_file_index + 1}.{compression}.parquet")
            writer = pq.ParquetWriter(parquet_file, schema=schema, compression=compression)
            local_parquet_files.append(parquet_file)
            current_file_index += 1
            current_size = 0

        table = pa.Table.from_pandas(chunk, schema=schema)
        writer.write_table(table)

        current_size += chunk.memory_usage().sum()

    if writer is None:
        parquet_file = os.path.join(parquet_dir, f"{base_name}_1.{compression}.parquet")
        writer = pq.ParquetWriter(parquet_file, schema=schema, compression=compression)
        local_parquet_files.append(parquet_file)

    return writer, local_parquet_files


def get_worker_num(source_info):
    worker_num = 4
    if re.search(r'imsi_\w+_1d', source_info.hdfs_dir):
        worker_num = 28
    elif re.search(r'imsi_\w+_1h', source_info.hdfs_dir):
        worker_num = 16
    logger.info(f'selected worker_num: {worker_num}, source_info.hdfs_dir: {source_info.hdfs_dir}')
    return worker_num



def get_field_names(schema_dict):
    return [field['name'] for field in schema_dict['fields']]


def get_schema(schema_dict):
    return pa.schema([pa.field(field['name'], _json_to_pyarrow_type(field['type'])) for field in schema_dict['fields']])


def _json_to_pyarrow_type(json_type):
    if json_type == 'string':
        return pa.string()
    elif json_type == 'int':
        return pa.int32()
    elif json_type == 'bigint':
        return pa.int64()
    elif json_type == 'float':
        return pa.float32()
    elif json_type == 'double':
        return pa.float64()
    elif json_type == 'boolean':
        return pa.bool_()
    elif json_type == 'timestamp':
        return pa.timestamp('ns')
    elif json_type == 'decimal':
        return pa.decimal128(30, 0)
    else:
        raise ValueError(f"Unsupported JSON type: {json_type}")


def _get_chunk_size_based_on_file_size(file_path, base_chunk_size=50000):
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    if file_size_mb > 1000:
        return int(base_chunk_size * 0.2)
    elif file_size_mb > 500:
        return int(base_chunk_size * 0.5)
    elif file_size_mb > 100:
        return base_chunk_size
    else:
        return int(base_chunk_size * 2)


def _initialize_message_params(interface_info: InterfaceInfo, source_info: SourceInfo, local_file_path):
    pass
    # target_time = time.time()
    # partitions = PathUtil.get_partition_dict_list(local_file_path)
    # if interface_info.protocol_cd in ['FTP', 'SFTP']:
    #     target_time = source_info.file_proto_info.target_time
    #
    # return target_time, partitions


def _initialize_conversion_params(interface_info: InterfaceInfo, source_info: SourceInfo):
    compression = None
    delimiter = None
    encode = "utf-8"
    merge = False

    if interface_info.protocol_cd in ['FTP', 'SFTP']:
        compression = source_info.file_proto_info.target_compression
        delimiter = _check_delimiter(interface_info.ftp_proto_info.delimiter)
        encode = interface_info.ftp_proto_info.encode
        merge = source_info.file_proto_info.merge
    elif interface_info.protocol_cd == 'DBCONN':
        # compression = source_info.db_proto_info.target_compression
        delimiter = bytes("\036", "utf-8").decode("unicode_escape")
        # merge = source_info.db_proto_info.merge
    elif interface_info.protocol_cd == 'DISTCP':
        compression = source_info.distcp_proto_info.target_compression
        delimiter = bytes("\036", "utf-8").decode("unicode_escape")
        merge = source_info.distcp_proto_info.merge

    return compression, delimiter, encode, merge


def _check_delimiter(delimiter):
    logger.info("checking delimiter")
    delimiter = bytes(delimiter, "utf-8").decode("unicode_escape")
    to_byte = delimiter.encode("utf-8")
    byte_size = len(to_byte)

    if byte_size > 1:
        logger.info("delimiter should be changed")
        if delimiter in ["|^|", ",|'", "\",\""]:
            changed_delimiter = bytes("\\u001e", "utf-8").decode("unicode_escape")
            logger.info("delimiter changed to %s -> %s", delimiter, changed_delimiter)
            return changed_delimiter
    else:
        return delimiter


def write_origin_file_to_info_dat(interface_info, info_dat_manager, local_file_path, hdfs_path, info_dat_path, ulid):
    if interface_info.ftp_proto_info:
        if (not interface_info.ftp_proto_info.external_process and not os.path.basename(local_file_path).endswith(".tar")
                and interface_info.ftp_proto_info.format != "xlsx"):
            info_dat_manager.write_info_dat(local_file_path, hdfs_path, os.path.basename(local_file_path),
                                            info_dat_path, ulid)
    else:
        info_dat_manager.write_info_dat(local_file_path, hdfs_path, os.path.basename(local_file_path),
                                        info_dat_path, ulid)


def _extract_table_name(hdfs_path):
    match = re.search(r'tb=([^/]+)', hdfs_path)
    if match:
        return match.group(1)
    else:
        raise Exception("There is no table name at hdfs path, info dat file")


def _convert_error_callbcak(error):
    logger.error(error)


def _send_message(local_files_parquet_msgs, source_info: SourceInfo, interface_info: InterfaceInfo, collect_history: CollectHistory, info_dat_path):
    for local_file, parquet_files in local_files_parquet_msgs.items():
        for parquet_local_file in parquet_files:
            target_time, paritions = _initialize_message_params(interface_info, source_info, parquet_local_file)
            parquet_info = ParquetInfoManager(source_info, parquet_local_file)
            parquet_info.complete_msg_info(target_time, paritions)
            logger.info("parquet_msg_info : %s", parquet_info.parquet_msg_info)
            ulid = parquet_info.send_kafka(collect_history)
            parquet_info.write_to_info_dat(info_dat_path, ulid)


def _decompress_gz_file(local_files):
    compression_manager = CompressionManager()
    result_files = []
    for local_file in local_files:
        if "_SUCCESS" in local_file:
            continue
        elif ".gz" in local_file:
            decompress_file = compression_manager.decompress_gz_with_zlib(local_file)
            result_files.append(decompress_file)
        else:
            result_files.append(local_file)
    return result_files


def create_mock_csv_file(directory, filename, num_rows=100):
    file_path = os.path.join(directory, filename)
    df = pd.DataFrame({
        'col1': range(num_rows),
        'col2': [f'string_{i}' for i in range(num_rows)],
        'col3': [Decimal(i) for i in range(num_rows)]
    })
    df.to_csv(file_path, index=False)
    return file_path


def main():
    with tempfile.TemporaryDirectory() as test_dir:
        csv_file1 = create_mock_csv_file(test_dir, 'test_file1.csv', num_rows=1000)
        csv_file2 = create_mock_csv_file(test_dir, 'test_file2.csv', num_rows=1000)

        source_info = SourceInfo()
        interface_info = InterfaceInfo()

        source_info.hdfs_dir = "/db=o_tpani_cem/tb=imsi_cell_1d"
        source_info.file_proto_info.target_compression = 'lz4'
        interface_info.protocol_cd = 'FTP'
        interface_info.ftp_proto_info.delimiter = ','
        interface_info.ftp_proto_info.encode = 'utf-8'

        schema_dict = SchemaInfoManager(source_info.hdfs_dir).get_schema_dict()

        try:
            with Pool(processes=min(5, cpu_count())) as pool:
                pool.starmap(convert, [(source_info, interface_info, local_file, schema_dict) for local_file in [csv_file1, csv_file2]])
        except Exception as e:
            logger.exception("Error in parallel conversion")
            raise e

        parquet_files = [f for f in os.listdir(test_dir) if f.endswith('.parquet')]
        print("Generated Parquet files:", parquet_files)
        assert len(parquet_files) > 0, "Parquet 파일이 생성되지 않았습니다."


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
