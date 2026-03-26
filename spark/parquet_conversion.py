import os
import sys
import json
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
import pyspark.sql.types as T
from pyspark import SparkFiles
import base64
from dataclasses import dataclass, asdict
from typing import Dict, List

from StandaloneCipherWrapper import StandaloneCipherWrapper

env = os.environ.copy()
env['PATH'] = '/local/HADOOP/bin:' + env.get('PATH', '')

cipher_instances = {}


@dataclass
class ParqeutConversionConfig:
    delimiter: str = None
    source_format:str = None
    target_format: str = None
    target_compression: str = None
    source_compression: str = None
    encoding: str = None
    header_skip: bool = False
    merge_to_one_file: bool = False
    merge_file_nm: str = None

def json_to_sql_type(json_type):
    mapping = {
        'string': T.StringType(),
        'int': T.IntegerType(),
        'bigint': T.LongType(),
        'float': T.FloatType(),
        'double': T.DoubleType(),
        'boolean': T.BooleanType(),
        'timestamp': T.TimestampType(),
        'decimal': T.LongType(),
        'datetime': T.TimestampType()
    }
    return mapping.get(json_type, T.StringType())  # 지원하지 않는 타입은 기본적으로 문자열로 처리

def json_to_spark_schema(json_schema):

    fields = [
        T.StructField(field["name"], json_to_sql_type(field["type"]))
        for field in json_schema["fields"]
    ]
    return T.StructType(fields)

def read_dataframe(spark, source_file_format, input_path_pattern, json_schema, delimiter, encoding, header_skip):
    df_reader = spark.read.format(source_file_format)
    if source_file_format in ["orc", "parquet"]:
        return df_reader.load(input_path_pattern)

    delimiter = bytes(delimiter, "utf-8").decode("unicode_escape")

    df_reader = (df_reader.option('sep', delimiter)
                 .option('encoding', encoding))

    if header_skip:
        df_reader = df_reader.option('header', "true")

    sys.stdout.write("seperate with delimiter\n")
    if json_schema:
        sys.stdout.write("add schema \n")
        spark_schema = json_to_spark_schema(json_schema)
        df_reader = df_reader.schema(spark_schema)

    return df_reader.load(input_path_pattern)


def read_schema_file(spark, schema_json_file_path,):
    json_data = spark.sparkContext.textFile(schema_json_file_path)
    json_str = "\n".join(json_data.collect())
    schema = json.loads(json_str)
    return schema

def is_path_contain_filename(spark, hdfs_path):
    """HDFS 경로가 파일인지 디렉토리인지 확인 (파일이면 True)"""
    sc = spark.sparkContext
    jvm = sc._jvm
    conf = sc._jsc.hadoopConfiguration()
    Path = jvm.org.apache.hadoop.fs.Path
    FileSystem = jvm.org.apache.hadoop.fs.FileSystem

    path = Path(hdfs_path)
    fs = FileSystem.get(conf)

    if not fs.exists(path):
        raise Exception(f"HDFS path does not exist: {hdfs_path}")

    # HDFS에서 실제로 파일인지 확인
    return fs.isFile(path)

def set_merge_file_nm(target_format, target_compression, merge_file_nm):
    """경로에서 파일명 생성 (파일인 경우만 호출됨, target 압축 확장자 추가)"""
    import os

    # 압축 형식과 확장자 매핑
    compression_extension_map = {
        'gzip': '.gz',
        'gz': '.gz',
        'zstd': '.zstd',
        'bzip2': '.bz2',
        'bz2': '.bz2',
        'zip': '.zip',
        'snappy': '.snappy'
    }

    # 압축이 있으면 파일명.압축.확장자 형식, 없으면 파일명.확장자 형식
    if target_compression and target_compression != 'none':
        comp_ext = compression_extension_map.get(target_compression.lower())
        if comp_ext:
            # 파일명.압축.확장자 형식 (예: file.gz.parquet)
            result_filename = f"{merge_file_nm}{comp_ext}.{target_format}"
        else:
            # 압축 확장자 매핑이 없으면 파일명.확장자 형식
            result_filename = f"{merge_file_nm}.{target_format}"
    else:
        # 압축이 없으면 파일명.확장자 형식 (예: file.parquet)
        result_filename = f"{merge_file_nm}.{target_format}"

    return result_filename

def rename_single_part_in_hdfs(output_dir, final_filename, spark):
    sc = spark.sparkContext
    jvm = sc._jvm
    conf = sc._jsc.hadoopConfiguration()
    Path = jvm.org.apache.hadoop.fs.Path
    FileSystem = jvm.org.apache.hadoop.fs.FileSystem

    dir_path = Path(output_dir)
    fs = FileSystem.get(conf)

    statuses = fs.listStatus(dir_path)
    part_files = [
        st.getPath() for st in statuses
        if st.isFile and st.getPath().getName().startswith("part-")
    ]

    if len(part_files) != 1:
        names = [p.toString() for p in part_files]
        raise Exception(f"Expect exactly 1 part-* file in {output_dir}, found {len(part_files)}:{names}")

    src = part_files[0]
    dst = Path(dir_path, final_filename)
    if fs.exists(dst):
        fs.delete(dst, True)
    ok = fs.rename(src, dst)
    if not ok:
        raise Exception(f"Rename failed : {src} -> {dst}")

    sys.stdout.write(f"renamed : {src} -> {dst}\n")

def main():
    try:
        source_path = sys.argv[1]
        output_path = sys.argv[2]
        tb_name = sys.argv[3]

        spark = SparkSession.builder.appName(f"encrypt_{tb_name}").getOrCreate()

        sys.stdout.write(f"source_path : {source_path}, output_path: {output_path}\n")

        convert_config = spark.sparkContext.getConf().get("spark.app.config")
        schema_file_path = spark.sparkContext.getConf().get("spark.schema.path")

        sys.stdout.write(f"convert_config : {convert_config}\n")
        sys.stdout.write(f"schema_file_path : {schema_file_path}\n")

        convert_json = json.loads(convert_config)

        parquet_conversion: ParqeutConversionConfig
        parquet_conversion = ParqeutConversionConfig(**convert_json)

        json_schema = None
        if schema_file_path:
            json_schema = read_schema_file(spark, schema_file_path)

        df = read_dataframe(spark, parquet_conversion.source_format, source_path, json_schema, parquet_conversion.delimiter, parquet_conversion.encoding, parquet_conversion.header_skip)

        output_path = os.getenv("OUTPUT_PATH", output_path)
        sys.stdout.write(f"output_path : {output_path}\n")

        # 파일명이 포함된 경우 단일 파일로 병합 (coalesce(1))
        if parquet_conversion.merge_to_one_file:
            df = df.coalesce(1)
            sys.stdout.write("merged to one file for filename preservation.\n")

        # target_compression이 None일 때 포맷별 기본값 설정
        target_compression = parquet_conversion.target_compression
        if target_compression is None:
            if parquet_conversion.target_format in ['csv', 'dat']:
                target_compression = "none"
            elif parquet_conversion.target_format in ['parquet', 'orc']:
                target_compression = "zstd"

        df.write.format(parquet_conversion.target_format).mode("overwrite").option("compression", target_compression).save(output_path)

        # 파일명이 포함된 경우 파일명 변경
        if parquet_conversion.merge_to_one_file:
            output_file_nm = set_merge_file_nm(parquet_conversion.target_format, target_compression, parquet_conversion.merge_file_nm)
            rename_single_part_in_hdfs(output_path, output_file_nm, spark)

    except Exception as e:
        sys.stderr.write(f"Error processing ORC file: {str(e)}\n")
        sys.stderr.write(traceback.format_exc())


if __name__ == "__main__":
    main()