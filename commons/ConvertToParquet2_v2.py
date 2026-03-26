import io
import logging
import os
import time
import warnings
import pandas as pd
import glob
import re
import polars as pl

import tempfile
from decimal import Decimal
from billiard import Pool, cpu_count
from commons.MetaInfoHook import InterfaceInfo, SourceInfo
from commons.SchemaInfoManager import SchemaInfoManager
from commons.CollectHistory import CollectHistory

from airflow.exceptions import AirflowException
from airflow.models import Variable

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))


warnings.simplefilter(action='ignore', category=FutureWarning)

def json_to_polars_type(json_type):
    mapping = {
        'string': pl.Utf8,
        'int': pl.Int32,
        'bigint': pl.Float64,
        'float': pl.Float32,
        'double': pl.Float64,
        'boolean': pl.Boolean,
        'timestamp': pl.Datetime,
        'decimal': pl.Int64,
        'datetime': pl.Datetime
    }
    return mapping.get(json_type, pl.Utf8)  # 지원하지 않는 타입은 기본적으로 문자열로 처리


def convert(csv_file_pattern, source_info: SourceInfo, interface_info: InterfaceInfo, collect_history: CollectHistory, info_dat_path):
    csv_files = glob.glob(csv_file_pattern)
    if not csv_files:
        logger.warning("No CSV files found matching the pattern.")
        return

    return convert_csv_to_parquet(source_info, interface_info, csv_file_pattern)


def convert_csv_to_parquet(source_info: SourceInfo, interface_info: InterfaceInfo,
                           csv_file_pattern, target_file_size=4 * 1024 * 1024 * 1024):
    start_time = time.time()
    local_files = _except_success_file_from_list(glob.glob(csv_file_pattern))
    compression, delimiter, encode, merge, prevent_split, has_header, quote_char = _initialize_conversion_params(interface_info, source_info)

    result_files = []

    if not local_files:
        logger.info(f"No files matching pattern {csv_file_pattern} found.")

    try:
        schema_dict = SchemaInfoManager(source_info.hdfs_dir).get_schema_dict()

        field_names = [field['name'] for field in schema_dict['fields']]
        field_types = [json_to_polars_type(field['type']) for field in schema_dict['fields']]

        schema = dict(zip(field_names, field_types))
        parquet_files = []
        total_csv_size = 0

        with Pool(processes=get_worker_num(source_info)) as pool:
            # TPDO-656: 인코딩 에러 알림용 interface_id, hdfs_dir 전달
            interface_id = getattr(interface_info, 'interface_id', None) or ''
            hdfs_dir = getattr(source_info, 'hdfs_dir', None) or ''
            tasks = [(csv_file, schema_dict, schema, field_names, target_file_size, compression, delimiter, encode, merge, prevent_split, has_header, quote_char, interface_id, hdfs_dir)
                     for csv_file in local_files]
            results = pool.map(process_csv_file, tasks, chunksize=1)
            pool.close()
            pool.join()

        for result, file_size in results:
            if file_size == 0:
                if interface_info.protocol_cd in ['DBCONN', 'DISTCP']:
                    raise AirflowException(f"Error at converting to parquet")
                else:
                    parquet_files.append(result)
            if isinstance(result, list):
                parquet_files.extend(result)
            else:
                if file_size > 0:
                    parquet_files.append(result)
            total_csv_size += file_size
        logger.info("total_csv_size: %s, len(paruqet_files) : %s", total_csv_size, len(parquet_files))

        logger.info("total_csv_size: %s, target_file_size: %s, merge value: %s", total_csv_size, target_file_size, merge)
        if (total_csv_size > 0 and total_csv_size <= target_file_size and len(parquet_files) > 1) or merge:
            logger.info("start merge, parquet_files size: %s", len(parquet_files))
            merge_parquet_files_path, merge_result = merge_parquet_files(parquet_files, compression)
            logger.info(merge_result)
            result_files.append(merge_parquet_files_path)
        else:
            result_files = parquet_files
            for msg in parquet_files:
                logger.info(msg)
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f'Execution time: {execution_time:.2f} secs')

        return result_files
    except FileNotFoundError as e:
        logger.error("There is no schema json file at hdfs://dataGWCluster/schema/")
        logger.error(e)
    except Exception as e:
        logger.exception("Error in conversion")
        raise e


def process_csv_file(args):
    # TPDO-656: 인코딩 에러 알림용 interface_id, hdfs_dir 수신
    csv_file, schema_dict, schema, field_names, target_file_size, compression, delimiter, encode, merge, prevent_split, has_header, quote_char, interface_id, hdfs_dir = args
    current_file_index = 0
    start_time = time.time()

    try:
        logger.info(f"file : {csv_file}")
        base_name = os.path.splitext(os.path.basename(csv_file))[0]
        parquet_file = csv_file.replace("/idcube_out", "/idcube_out/converted_parquet")
        parquet_dir = os.path.dirname(parquet_file)
        os.makedirs(parquet_dir, exist_ok=True)

        if os.stat(csv_file).st_size == 0:
            logger.info(f"CSV file {csv_file} is empty. Creating empty Parquet file.")
            empty_df = pl.DataFrame({name: pl.Series(name, [], dtype=schema[name]) for name in field_names})
            parquet_file = os.path.join(parquet_dir,
                                        f"{base_name}_{current_file_index + 1}.{compression}.parquet")
            empty_df.write_parquet(parquet_file, compression=compression)
            logger.info(f"Empty Parquet file created: {parquet_file}")
            return parquet_file, 0

        last_column = field_names[-1]
        schema_with_last_column_as_string = schema.copy()
        schema_with_last_column_as_string[last_column] = pl.Utf8
        logger.info("reading %s", csv_file)
        # TPDO-656: 인코딩 에러 시 로그·문자 알림 후 수동 decode(replace) 로 재시도 (Polars encoding_errors 미지원 버전 대응)
        try:
            df = pl.read_csv(
                csv_file,
                separator=delimiter,
                encoding=encode,
                has_header=has_header,
                new_columns=field_names,
                schema_overrides=schema_with_last_column_as_string,
                null_values=["", " "],
                quote_char=quote_char
            )
        except UnicodeDecodeError as e:
            logger.warning("encoding error during read (%s), retrying with decode errors=replace: %s", csv_file, e)
            try:
                from commons.SmsSender import send_encoding_error_alert
                send_encoding_error_alert(csv_file, e, interface_id=interface_id, hdfs_dir=hdfs_dir)
            except Exception:
                pass
            with open(csv_file, 'rb') as f:
                raw = f.read()
            text = raw.decode(encode, errors='replace')
            df = pl.read_csv(
                io.BytesIO(text.encode('utf-8')),
                separator=delimiter,
                encoding='utf-8',
                has_header=has_header,
                new_columns=field_names,
                schema_overrides=schema_with_last_column_as_string,
                null_values=["", " "],
                quote_char=quote_char
            )
        logger.info("finished reading %s", csv_file)
        df = df.with_columns(pl.col(last_column).str.replace(r'\t$', '').cast(schema[last_column]))

        bigint_columns = [field['name'] for field in schema_dict['fields'] if field['type'] == 'bigint']
        for col in bigint_columns:
            df = df.with_columns(pl.col(col).floor().cast(pl.Int64))

        total_rows = df.height
        total_size = df.to_arrow().nbytes

        ## 파일 사이즈가 4GB보다 작으면은 그냥 parquet 변환 처리
        if total_size <= target_file_size or prevent_split:
            parquet_file = os.path.join(parquet_dir,
                                        f"{base_name}_{current_file_index + 1}.{compression}.parquet")
            logger.info("parquet_file : %s", parquet_file)
            df.write_parquet(parquet_file, compression=compression)

            end_time = time.time()
            execution_time = end_time - start_time
            logger.info('%s Execution time: %.2f secs', parquet_file, execution_time)
            return parquet_file, total_size
        else:
            num_chunks = (total_size // target_file_size) + 1
            rows_per_chunk = total_rows // num_chunks

            parquet_files = []
            for part in range(num_chunks):
                start_row = part * rows_per_chunk
                end_row = min((part + 1) * rows_per_chunk, total_rows)

                df_chunk = df.slice(start_row, end_row - start_row)
                parquet_file = os.path.join(parquet_dir,
                                            f"{base_name}_{current_file_index + 1}.{compression}.parquet")
                logger.info(f"parquet_file : {parquet_file}")
                df_chunk.write_parquet(parquet_file, compression=compression)
                parquet_files.append(parquet_file)
                current_file_index+=1

            end_time = time.time()
            execution_time = end_time - start_time
            logger.info('%s Execution time: %.2f secs', parquet_file, execution_time)
            return parquet_files, total_size
    except Exception as e:
        logger.exception(f"convertion error, file : {csv_file}")
        return f"Error processing file {csv_file}: {e}", 0


def merge_parquet_files(parquet_files, compression):
    try:
        parquet_dir = os.path.dirname(parquet_files[0])
        merge_file = f"merged_output.{compression}.parquet"
        merge_file_path = os.path.join(parquet_dir, merge_file)
        logger.info("merge parquet_file %s", merge_file_path)

        dfs = [pl.read_parquet(f) for f in parquet_files]
        merged_df = pl.concat(dfs)

        merged_df.write_parquet(merge_file_path, compression=compression)
        remove_origin_file(merge_file_path)
        logger.info(f"Merge file created : {merge_file_path}")

        return merge_file_path, f"Merged file created: {merge_file_path}"
    except Exception as e:
        logger.exception("Error during merge, file : %s",)
        return f"Error merging files: {e}"


def remove_origin_file(merge_file_path):
    logger.info("start removing origin local file")
    parquet_dir = os.path.dirname(merge_file_path)
    merge_file_name = os.path.basename(merge_file_path)

    for file_name in os.listdir(parquet_dir):
        file_path = os.path.join(parquet_dir, file_name)
        if file_name != merge_file_name and os.path.isfile(file_path):
            os.remove(file_path)
            logger.info("remove origin file: %s", file_path)


def _initialize_conversion_params(interface_info: InterfaceInfo, source_info: SourceInfo):
    compression = None
    delimiter = None
    encode = "utf-8"
    merge = False
    prevent_split = False
    has_header = False
    quote_char = '"'

    if interface_info.protocol_cd in ['FTP', 'SFTP']:
        compression = source_info.file_proto_info.target_compression
        delimiter = _check_delimiter(interface_info.ftp_proto_info.delimiter)
        encode = interface_info.ftp_proto_info.encode
        merge = source_info.file_proto_info.merge
        prevent_split = source_info.file_proto_info.prevent_split

    elif interface_info.protocol_cd == 'DBCONN':
        compression = source_info.db_proto_info.target_compression
        delimiter = bytes("\036", "utf-8").decode("unicode_escape")
        merge = source_info.db_proto_info.merge

        if source_info.db_proto_info.contain_quote:
            logger.info("contain_quote: ''")
            quote_char = ''

    elif interface_info.protocol_cd == 'DISTCP':
        compression = source_info.distcp_proto_info.target_compression
        delimiter = _check_delimiter(interface_info.distcp_proto_info.delimiter)
        merge = source_info.distcp_proto_info.merge
        has_header = source_info.distcp_proto_info.header_skip
        encode = "utf8"

    elif interface_info.protocol_cd == 'KAFKA':
        compression = source_info.kafka_proto_info.target_compression
        encode = source_info.kafka_proto_info.encode
        delimiter = bytes(source_info.kafka_proto_info.delimiter, "utf-8").decode("unicode_escape")

    return compression, delimiter, encode, merge, prevent_split, has_header, quote_char

def _except_success_file_from_list(local_files):
    result_files = []
    for local_file in local_files:
        if "_SUCCESS" in local_file or "":
            continue
        result_files.append(local_file)
    return result_files


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

def get_worker_num(source_info):
    worker_num = 2
    logger.info(f'selected worker_num: {worker_num}, source_info.hdfs_dir: {source_info.hdfs_dir}')
    return worker_num


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
