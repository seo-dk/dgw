import logging
import re
import signal
import sys
import shutil
import pathlib
import subprocess
from datetime import datetime
import time
import os
import glob
import shutil
import polars as pl

from commons.ProvideMetaInfoHook import ProvideInterface
import commons.HdfsUtil as HdfsUtil

from airflow.exceptions import AirflowException
from airflow.models import Variable

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))

class ConvertToCsv:
    def __init__(self, hdfs_src_path, provide_interface: ProvideInterface, clear_path):
        self.hdfs_src_path = hdfs_src_path
        self.provide_interface = provide_interface
        self.is_hdfs_path_contain_file_name = False
        self.partitions = None
        self.clear_path = clear_path

        if HdfsUtil.is_file_path(self.hdfs_src_path):
            logger.info("hdfs src path contain file name")
            self.local_download_path = '/data' + os.path.dirname(self.hdfs_src_path)
            self.hdfs_dest_path = '/staging' + os.path.dirname(self.hdfs_src_path)
            self.is_hdfs_path_contain_file_name = True
        else:
            self.local_download_path = '/data' + self.hdfs_src_path
            self.hdfs_dest_path = '/staging' + self.hdfs_src_path

        self.local_csv_path = self.local_download_path.replace("/idcube_out", "/idcube_out/converted_csv")

        logger.info("local_download_path: %s, local_csv_path: %s, hdfs_dest_path: %s", self.local_download_path, self.local_csv_path, self.hdfs_dest_path)

    def execute_convert(self):
        if not self.provide_interface.proto_info_json.delimiter:
            delimiter = bytes("\\u001e", "utf-8").decode("unicode_escape")
        else:
            delimiter = self.provide_interface.proto_info_json.delimiter
            delimiter = bytes(delimiter, "utf-8").decode("unicode_escape")

        need_merge = self.provide_interface.proto_info_json.merge

        if self.provide_interface.proto_info_json.add_partitions:
            logger.info("add partitions true")
            self.partitions = self._get_partition(self.hdfs_src_path)

        logger.info("provide delimiter: %s", self.provide_interface.proto_info_json.delimiter)
        self._download(self.hdfs_src_path, self.local_download_path)
        self._convert_parquet_to_csv(self.local_download_path, self.local_csv_path, delimiter, need_merge)

        # for retry
        HdfsUtil.remove_files(self.hdfs_dest_path)

        hdfs_path = "hdfs://dataGWCluster" + self.hdfs_dest_path.rsplit('/', 1)[0]
        HdfsUtil.upload_files(self.local_csv_path, hdfs_path)

        return self.hdfs_dest_path


    def _download(self, src_path, dest_path):
        logger.info(f'downloading files,{src_path} to {dest_path}')
        logger.info(f"clear_path : {self.clear_path}")
        success_file_path = pathlib.Path(dest_path) / '_SUCCESS'
        if not self.clear_path and success_file_path.exists():
            logger.info("_SUCCESS file already exists at: %s. Skipping download.", success_file_path)
            return

        if os.path.exists(dest_path):
            shutil.rmtree(dest_path)
            logger.info("local path removed for retry: %s", dest_path)

        os.makedirs(dest_path, exist_ok=True)

        if not self.is_hdfs_path_contain_file_name:
            HdfsUtil.get_files(os.path.join(src_path, "*"), dest_path)
        else:
            HdfsUtil.get_files(src_path, dest_path)

        self._validate_file_count(src_path, dest_path)


    def _convert_parquet_to_csv(self, local_download_path, local_csv_path, delimiter, need_merge=False):
        try:
            start_time = time.time()

            if os.path.exists(local_csv_path):
                shutil.rmtree(local_csv_path)
                logger.info("local path removed for retry: %s", local_csv_path)

            os.makedirs(local_csv_path, exist_ok=True)

            parquet_files = self._get_parquet_files(local_download_path)

            if need_merge:
                self._process_merge_parquet_files(parquet_files, local_csv_path, delimiter)
            else:
                logger.info("start convert to csv files")
                # parquet_file is full path
                for parquet_file in parquet_files:
                    self._process_parquet_files(parquet_file, delimiter)

            end_time = time.time()
            execution_time = end_time - start_time
            logger.info(f'Total execution time for all files: {execution_time:.2f} secs')
        except Exception as e:
            AirflowException(f"Failed to convert {local_download_path}: {e}")


    def _get_parquet_files(self, local_download_path):
        parquet_files = []

        for dir_path, _, file_names in os.walk(local_download_path):
            for file_name in file_names:
                if file_name != '_SUCCESS':
                    full_path = os.path.join(dir_path, file_name)
                    parquet_files.append(full_path)

        logger.info("parquet_files %s", parquet_files)
        return parquet_files

    def _process_parquet_files(self, parquet_file, delimiter):
        try:
            start_time = time.time()
            base_name = os.path.splitext(os.path.basename(parquet_file))[0]
            base_name = base_name.replace(".zstd", "")
            logger.info("parquet_file %s", parquet_file)

            local_csv_path = os.path.dirname(parquet_file).replace("/idcube_out", "/idcube_out/converted_csv")
            logger.info("local csv path %s", local_csv_path)

            os.makedirs(local_csv_path, exist_ok=True)

            csv_file = os.path.join(str(local_csv_path), f"{base_name}.csv")
            logger.info(f"csv_file : {csv_file}")

            df = pl.read_parquet(parquet_file)

            if self.partitions:
                df = self._insert_partitons(df)

            df = self._change_timezone(df)

            if self.provide_interface.proto_info_json.has_string_null:
                logger.info(f'This data has string null, "null" -> None')
                df = self._change_string_null_to_none(df)

            df.write_csv(
                csv_file,
                separator=delimiter,
                null_value='',
                include_header=False
            )

            execution_time = time.time() - start_time
            logger.info(f'{csv_file} is created')
            logger.info(f'Execution time for {parquet_file}: {execution_time:.2f} secs')

        except Exception as e:
            AirflowException(f"Failed to convert {parquet_file}: {e}")


    def _process_merge_parquet_files(self, parquet_files, local_csv_path, delimiter):
        try:
            logger.info("start convert to merged csv file")

            start_time = time.time()

            merge_file_name = f"merged_output.csv"
            csv_file = os.path.join(local_csv_path, merge_file_name)

            df = pl.concat([pl.read_parquet(file) for file in parquet_files])
            logger.info(f"merge_parqet_line : {df.shape[0]}")
            if self.partitions:
                df = self._insert_partitons(df)
            df = self._change_timezone(df)

            if delimiter == "\u241e":
                self._handle_mulite_byte_delimiter(df, csv_file, delimiter)
            else:
                df.write_csv(
                    csv_file,
                    separator=delimiter,
                    null_value='',
                    include_header=False
                )

            execution_time = time.time() - start_time
            logger.info(f'{csv_file} is created')
            if len(parquet_files) > 100:
                logger.info(f'Execution time for {len(parquet_files)} parquet_files: {execution_time:.2f} secs.. since file size more than 100, so specify only the size')
            else:
                logger.info(f'Execution time for {parquet_files}: {execution_time:.2f} secs')

        except Exception as e:
            AirflowException(f"Failed to convert {self.local_download_path}: {e}")

    def _validate_file_count(self, src_path, dest_path):
        if not self.is_hdfs_path_contain_file_name:
            src_file_count = HdfsUtil.get_list_count(src_path)
            src_success_file_path = pathlib.Path(src_path) / '_SUCCESS'

            if HdfsUtil.is_file_path(src_success_file_path):
                src_file_count -= 1

        else:
            src_file_count = 1

        success_file_path = pathlib.Path(dest_path) / '_SUCCESS'

        if success_file_path.exists():
            os.remove(success_file_path)
            logger.info("Delete '_SUCCESS' file that was downloaded along with using the hdfs get command, path: %s", success_file_path)

        local_file_count = self._get_local_file_count(dest_path)
        logger.info("count of src files count: %s, local files count: %s", src_file_count, local_file_count)

        if src_file_count == local_file_count:
            success_file_path.touch()
            logger.info("count of src files and local files is correct, count: %s", local_file_count)
            logger.info("_SUCCESS file created at: %s", success_file_path)
        else:
            raise AirflowException(f"count of src files and downloaded files is different, src_path: '{src_path}', src_file_count: '{src_file_count}', "
                                   f"local_path: '{dest_path}', local_file_count: '{local_file_count}'")

    def _get_local_file_count(self, path):
        result = subprocess.run(f'find {path} -type f | wc -l', shell=True, capture_output=True, text=True)
        return int(result.stdout.strip())

    def _get_partition(self, path):
        partitions_list = []

        match = re.search(r"/dt=(\d+)(?:/hh=(\d+))?", path)
        if not match:
            raise ValueError("Input path does not contain the expected dt/hh structure")

        dt, hh = match.groups()

        if hh:
            partitions_list.append({'dt': dt})
            partitions_list.append({'hh': hh})
        else:
            partitions_list.append({'dt': dt})

        logger.info("partitions_list: %s", partitions_list)

        return partitions_list


    def _insert_partitons(self, df):
        logger.info("insert partitons into data frame")

        for dict in self.partitions:
            for key, value in dict.items():
                if key == 'dt':
                    df = df.with_columns(pl.lit(value).cast(pl.Utf8).alias("dt"))
                    logger.info("dt_value added: %s", value)
                    continue

                if key == 'hh':
                    df = df.with_columns(pl.lit(value).cast(pl.Utf8).alias("hh"))
                    logger.info("hh_value added: %s", value)
                    continue

        df = df.select(pl.all().cast(pl.Utf8))

        return df

    def get_path(self):
        return self.local_download_path, self.local_csv_path, self.hdfs_dest_path

    def _change_timezone(self, df):
        for column in df.columns:
            if df[column].dtype == pl.Datetime:
                df = df.with_columns(pl.col(column).dt.convert_time_zone('Asia/Seoul'))

        return df.select(pl.all().cast(pl.Utf8))

    def _handle_mulite_byte_delimiter(self, df, csv_file, delimiter):
        logger.info("delimiter is %s", delimiter)
        with open(csv_file, "w", encoding='utf-8') as f:
            buffer_size = 100_000
            for batch in df.iter_rows(buffer_size=buffer_size):
                csv_batch = ",".join(str(v) for v in batch)
                csv_batch = csv_batch.replace(",", delimiter)
                f.write(csv_batch + "\n")


    def _change_string_null_to_none(self, df):
        string_cols = [col for col, dtype in zip(df.columns, df.dtypes) if dtype == pl.Utf8]
        df = df.with_columns([
            pl.when(pl.col(col) == "null").then(None).otherwise(pl.col(col)).alias(col)
            for col in string_cols
        ])
        return df

if __name__ == '__main__':
    pass
