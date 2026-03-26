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

from commons.InfodatManager import InfoDatManager
from commons.MetaInfoHook import InterfaceInfo, SourceInfo
from commons.SchemaInfoManager import SchemaInfoManager
from commons.KafkaSend import KafkaSend
from commons.ParquetInfoManager import ParquetInfoManager, ParquetMessageInfo
from commons.CollectHistory import CollectHistory

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


def convert_sequentially(source_info, interface_info, local_file_pattern, partition, collect_history: CollectHistory, info_dat_path, target_time):
    schema_dict = SchemaInfoManager(source_info.hdfs_dir).get_schema_dict()
    local_files = glob.glob(str(local_file_pattern))
    try:
        with Pool(processes=min(1, cpu_count())) as pool:
            local_files_parquet_msgs = pool.starmap(_convert2, [(source_info, interface_info, local_files, schema_dict)])
            # local_files_dict = local_files_parquet_msgs[0]
            for local_files_parquet_msg in local_files_parquet_msgs:
                for parquet_local_path, parquet_infos in local_files_parquet_msg.items():  # parquet_info = ParquetInfoManager
                    logger.info("parquet_local_path : %s", parquet_local_path)

                    for parquet_info in parquet_infos:
                        parquet_info.complete_msg_info(target_time, partition)
                        logger.info("parquet_msg_info : %s", parquet_info.parquet_msg_info)
                        ulid = parquet_info.send_kafka(collect_history)
                        parquet_info.write_to_info_dat(info_dat_path, ulid)
    except Exception as e:
        logger.exception("Error in parallel conversion")
        raise
    # _convert(source_info, interface_info, local_files, schema_dict)


def convert_in_parallel(source_info, interface_info, local_files):
    schema_dict = SchemaInfoManager(source_info.hdfs_dir).get_schema_dict()
    try:
        worker_num = get_worker_num(source_info)
        with Pool(processes=worker_num) as pool:
            pool.starmap(_convert, [(source_info, interface_info, local_file, schema_dict) for local_file in local_files], chunksize=1)
            pool.close()
            pool.join()
    except Exception as e:
        logger.exception("Error in parallel conversion")
        raise

def get_worker_num(source_info):
    worker_num = 1
    if re.search(r'imsi_\w+_1d', source_info.hdfs_dir):
        worker_num = 25
    elif re.search(r'imsi_\w+_1h', source_info.hdfs_dir):
        worker_num = 6
    logger.info(f'selected worker_num: {worker_num}, source_info.hdfs_dir: {source_info.hdfs_dir}')
    return worker_num

def _convert(source_info, interface_info, local_files, schema_dict, parquet_size_mb=2000):
    converters = get_converters(schema_dict)
    compression, delimiter, encode = _initialize_conversion_params(interface_info, source_info)

    try:
        start_time = time.time()

        if isinstance(local_files, str):
            local_files = [local_files]

        local_files = [file for file in local_files if os.path.basename(file) != '_SUCCESS']

        common_path = os.path.dirname(local_files[0])
        file_names = [os.path.basename(file) for file in local_files]

        logger.info('[%s] started to convert files: %s', common_path, ', '.join(file_names))

        base_name = os.path.splitext(os.path.basename(local_files[0]))[0]
        writer = None
        for local_file in local_files:
            writer = _convert_and_write(base_name, local_file, parquet_size_mb, writer, converters, delimiter, compression, schema_dict, encode)

        if writer is not None:
            writer.close()

        end_time = time.time()
        execution_time = end_time - start_time
        logger.info('Files converted to Parquet with approximately %s MB per file', parquet_size_mb)
        logger.info('[%s] [%s] Execution time: %.2f secs', common_path, ', '.join(file_names), execution_time)
    except Exception as e:
        logger.exception("Error converting %s", local_files, e)
        raise Exception(f"Failed to convert {local_files}") from e

def _convert2(source_info, interface_info, local_files, schema_dict, parquet_size_mb=2000):
    converters = get_converters(schema_dict)
    compression, delimiter, encode = _initialize_conversion_params(interface_info, source_info)

    logger.info('[%s] started to convert', local_files)
    try:
        start_time = time.time()

        if isinstance(local_files, str):
            local_files = [local_files]

        base_name = os.path.splitext(os.path.basename(local_files[0]))[0]
        writer = None
        local_files_parquet_msgs = {}

        for local_file in local_files:
            writer, parquet_local_files = _convert_and_write2(base_name, local_file, parquet_size_mb, writer, converters, delimiter, compression, schema_dict, encode)
            parquet_infos = []
            for parquet_local_file in parquet_local_files:
                ##여기서 local_parquet file은 local 경로임. 로컬파일 하나당 생성된 parquet file 의 list
                parquet_info = ParquetInfoManager(source_info, parquet_local_file)
                parquet_infos.append(parquet_info)
            local_files_parquet_msgs[local_file] = parquet_infos
        logger.info("local_files_parquet_msgs : %s", local_files_parquet_msgs)
        if writer is not None:
            writer.close()
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info('Files converted to Parquet with approximately %s MB per file', parquet_size_mb)
        logger.info('%s Execution time: %.2f secs', local_files, execution_time)

        ## need to send list at here
        return local_files_parquet_msgs
    except Exception as e:
        logger.exception("Error converting %s", local_files, e)
        raise Exception(f"Failed to convert {local_files}") from e



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

def _convert_and_write(base_name, local_file, parquet_size_mb, writer, converters, delimiter, compression, schema_dict, encode):
    current_file_index = 0
    current_size = 0

    chunksize= _get_chunk_size_based_on_file_size(local_file)

    parquet_file = local_file.replace("/idcube_out", "/idcube_out/converted")

    try:
        first_chunk = pd.read_csv(local_file, header=None, names=get_field_names(schema_dict),
                                  converters=converters, sep=delimiter, encoding=encode, na_values=['', ' '],
                                  engine='python', chunksize=chunksize).__next__()
    except StopIteration:
        first_chunk = pd.DataFrame()

    if first_chunk.empty:
        local_parquet_full_path = os.path.join(os.path.dirname(parquet_file),
                                    f"{base_name}_{current_file_index + 1}.{compression}.parquet")
        local_parquet_dir = os.path.dirname(local_parquet_full_path)
        if not os.path.exists(local_parquet_dir):
            os.makedirs(local_parquet_dir)

        writer.close()
        return writer

    for chunk in pd.read_csv(local_file, header=None, names=get_field_names(schema_dict),
                             converters=converters, sep=delimiter, encoding=encode, na_values=['', ' '],
                             engine='python', chunksize=chunksize):
        while not chunk.empty:
            if writer is None or current_size >= parquet_size_mb * 1024 * 1024:
                if writer is not None:
                    writer.close()
                local_parquet_full_path = os.path.join(os.path.dirname(parquet_file),
                                                       f"{base_name}_{current_file_index + 1}.{compression}.parquet")
                local_parquet_dir = os.path.dirname(local_parquet_full_path)
                if not os.path.exists(local_parquet_dir):
                    os.makedirs(local_parquet_dir)
                writer = pq.ParquetWriter(local_parquet_full_path, schema=get_schema(schema_dict), compression=compression)
                current_file_index += 1
                current_size = 0

            table = pa.Table.from_pandas(chunk, schema=get_schema(schema_dict))
            writer.write_table(table)

            current_size += chunk.memory_usage().sum()
            chunk = pd.DataFrame()

    return writer


def _convert_and_write2(base_name, local_file, parquet_size_mb, writer, converters, delimiter, compression, schema_dict, encode, chunksize=100000):
    local_parquet_files = []

    current_file_index = 0
    current_size = 0

    parquet_file = local_file.replace("/idcube_out", "/idcube_out/converted")

    try:
        first_chunk = pd.read_csv(local_file, header=None, names=get_field_names(schema_dict),
                                  converters=converters, sep=delimiter, encoding=encode, na_values=['', ' '],
                                  engine='python', chunksize=chunksize).__next__()
    except StopIteration:
        first_chunk = pd.DataFrame()

    if first_chunk.empty:
        local_parquet_full_path = os.path.join(os.path.dirname(parquet_file),
                                    f"{base_name}_{current_file_index + 1}.{compression}.parquet")
        local_parquet_dir = os.path.dirname(local_parquet_full_path)
        if not os.path.exists(local_parquet_dir):
            os.makedirs(local_parquet_dir)
        writer = pq.ParquetWriter(local_parquet_full_path, schema=get_schema(schema_dict), compression=compression)
        local_parquet_files.append(local_parquet_full_path)
        writer.close()
        return writer, local_parquet_files

    for chunk in pd.read_csv(local_file, header=None, names=get_field_names(schema_dict),
                             converters=converters, sep=delimiter, encoding=encode, na_values=['', ' '],
                             engine='python', chunksize=chunksize):
        while not chunk.empty:
            if writer is None or current_size >= parquet_size_mb * 1024 * 1024:
                if writer is not None:
                    writer.close()
                local_parquet_full_path = os.path.join(os.path.dirname(parquet_file),
                                                       f"{base_name}_{current_file_index + 1}.{compression}.parquet")
                local_parquet_dir = os.path.dirname(local_parquet_full_path)
                if not os.path.exists(local_parquet_dir):
                    os.makedirs(local_parquet_dir)
                writer = pq.ParquetWriter(local_parquet_full_path, schema=get_schema(schema_dict), compression=compression)
                local_parquet_files.append(local_parquet_full_path)
                current_file_index += 1
                current_size = 0

            table = pa.Table.from_pandas(chunk, schema=get_schema(schema_dict))
            writer.write_table(table)

            current_size += chunk.memory_usage().sum()
            chunk = pd.DataFrame()
    if not local_parquet_files:
        logger.info("no parquet files are made..., local_file : %s", local_file)

    return writer, local_parquet_files


def _initialize_conversion_params(interface_info: InterfaceInfo, source_info: SourceInfo):
    compression = None
    delimiter = None
    encode = "utf-8"

    if interface_info.protocol_cd in ['FTP', 'SFTP']:
        compression = source_info.file_proto_info.target_compression
        delimiter = _check_delimiter(interface_info.ftp_proto_info.delimiter)
        encode = interface_info.ftp_proto_info.encode
    elif interface_info.protocol_cd == 'DBCONN':
        # compression = source_info.db_proto_info.target_compression
        delimiter = bytes("\036", "utf-8").decode("unicode_escape")
    elif interface_info.protocol_cd == 'DISTCP':
        compression = source_info.distcp_proto_info.target_compression
        delimiter = bytes("\036", "utf-8").decode("unicode_escape")

    return compression, delimiter, encode


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


def _convert_callback(parquet_infos):
    logger.info("")
    kafka_send = KafkaSend()


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
                pool.starmap(convert_in_parallel, [(source_info, interface_info, local_file, schema_dict) for local_file in [csv_file1, csv_file2]])
        except Exception as e:
            logger.exception("Error in parallel conversion")
            raise e

        parquet_files = [f for f in os.listdir(test_dir) if f.endswith('.parquet')]
        print("Generated Parquet files:", parquet_files)
        assert len(parquet_files) > 0, "Parquet 파일이 생성되지 않았습니다."


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
