import os
import sys
import json
import traceback
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit
import pyspark.sql.types as T
import base64
from StandaloneCipherWrapper import StandaloneCipherWrapper

import ssl
import urllib.request
from urllib.parse import urlparse


env = os.environ.copy()
env['PATH'] = '/local/HADOOP/bin:' + env.get('PATH', '')

cipher_instances = {}


def get_cipher(enc_type, cipher_keys):
    global cipher_instances
    if enc_type not in cipher_instances:
        cipher_instances[enc_type] = StandaloneCipherWrapper(cipher_keys)
    return cipher_instances[enc_type]


def create_encrypt_udf(crypto_column_type, cipher_keys, crypto_type):
    def encrypt(value):
        if not value:
            return None
        try:
            cipher = get_cipher(crypto_column_type, cipher_keys)
            if crypto_type == "encryption":
                return cipher.encrypt(crypto_column_type, value)
            elif crypto_type == "decryption":
                return cipher.decrypt(crypto_column_type, value)
        except Exception as e:
            sys.stderr.write(f"Error encrypting value: {value} with type: {crypto_column_type}\n")
            return None

    return encrypt


def process_file(df, enc_rules, cipher_keys_broadcast, crypto_type):
    try:
        cipher_keys = cipher_keys_broadcast.value
        for rule in enc_rules:
            crypto_column_type = rule["type"]
            for col_pos in rule["col_pos"]:
                column_name = df.columns[col_pos - 1]
                encrypt_udf = udf(create_encrypt_udf(crypto_column_type, cipher_keys, crypto_type), T.StringType())
                df = df.withColumn(column_name, encrypt_udf(col(column_name)))

        return df
    except Exception as e:
        sys.stderr.write(f"Error during batch processing: {str(e)}\n")
        sys.stderr.write(traceback.format_exc())
        return None

def decode_to_json(context):
    decoded_string = base64.b64decode(context).decode()
    decoded_json = json.loads(decoded_string)
    return decoded_json


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


def read_dataframe(spark, source_file_format, input_path_pattern, json_schema, delimiter, encoding):
    df_reader = spark.read.format(source_file_format)
    if source_file_format in ["orc", "parquet"]:
        return df_reader.load(input_path_pattern)

    df_reader = (df_reader.option('sep', delimiter)
                 .option('encoding', encoding))

    sys.stdout.write("seperate with delimiter\n")
    if json_schema:
        sys.stdout.write("add schema \n")
        spark_schema = json_to_spark_schema(json_schema)
        df_reader = df_reader.schema(spark_schema)

    return df_reader.load(input_path_pattern)


def add_date_partition_from_path(df, input_path_pattern):
    partitions = {}
    for key in ["dt","hh","hm"]:
        match = re.search(rf"{key}=([^/]+)", input_path_pattern)
        if match:
            partitions[key] = match.group(1)

    for key, val in partitions.items():
        df = df.withColumn(key, lit(val))

    return df

def write_dataframe(encrypted_df, target_format, compression, delimiter, output_path):
    if target_format in ['csv', 'dat']:
        if compression:
            target_compression = compression
        else:
            target_compression = "none"
        writer = encrypted_df.write.format(target_format).mode("overwrite").option("compression", target_compression).option('sep', delimiter)
    elif target_format in ['orc', 'parquet', 'avro']:
        writer = encrypted_df.write.format(target_format).mode("overwrite").option("compression", compression)
    #
    # partitions_cols = [c for c in ["dt", "hh", "hm"] if c in encrypted_df.columns and c not in output_path]
    # if partitions_cols:
    #     writer = writer.partitionBy(*partitions_cols)
    writer.save(str(output_path))


def get_filename_from_path(path, target_format):
    tb_match = re.search("/tb=([^/]+)", path)
    dt_match = re.search("/dt=([^/]+)", path)
    hh_match = re.search("/hh=([^/]+)", path)
    hm_match = re.search("/hm=([^/]+)", path)

    tb = tb_match.group(1) if tb_match else None
    dt = dt_match.group(1) if dt_match else None
    hh = hh_match.group(1) if hh_match else None
    hm = hm_match.group(1) if hm_match else None

    parts = [tb]
    if dt:
        parts.append(dt)
    if hh:
        parts.append(hh)
    if hm:
        parts.append(hm)

    return "_".join(parts) + f".{target_format}"

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
        raise Exception(f"Expect exactly 1 part-* file in {output_dir}, fount {len(part_files)}:{names}")

    src = part_files[0]
    dst = Path(dir_path, final_filename)
    if fs.exists(dst):
        fs.delete(dst,True)
    ok = fs.rename(src, dst)
    if not ok:
        raise Exception(f"Rename failed : {src} -> {dst}")

    sys.stdout.write(f"renamed : {src} -> {dst}\n")

def get_ciphers_keys(VAULT_ADDR, VAULT_IP, VAULT_NAMESPACE, VAULT_TOKEN, VAULT_SECRET_PATH):
    parsed = urlparse(VAULT_ADDR)
    hostname = parsed.hostname  # test.com
    port = parsed.port          # 8200

    # IP로 접속하면서 Host 헤더로 hostname 명시
    url = f"https://{VAULT_IP}:{port}/v1/{VAULT_SECRET_PATH}"
    req = urllib.request.Request(url, headers={
        "Host": hostname,               # ingress 라우팅용
        "X-Vault-Token": VAULT_TOKEN,
        "X-Vault-Namespace": VAULT_NAMESPACE,
    })
    ctx = ssl._create_unverified_context()
    with urllib.request.urlopen(req, context=ctx) as resp:
        return json.loads(resp.read())["data"]["data"]


def main():
    try:
        VAULT_ADDR  = os.environ['VAULT_ADDR']
        VAULT_IP    = os.environ['VAULT_IP']
        VAULT_NAMESPACE = os.environ['VAULT_NAMESPACE']
        VAULT_TOKEN = os.environ['VAULT_TOKEN']
        VAULT_SECRET_PATH = os.environ['VAULT_SECRET_PATH']
        ENC_RULES = os.environ['ENC_RULES']

        input_path_pattern = sys.argv[1]
        output_path = sys.argv[2]
        ciphers_keys = get_ciphers_keys(VAULT_ADDR, VAULT_IP,  VAULT_NAMESPACE, VAULT_TOKEN, VAULT_SECRET_PATH)
        enc_rules = decode_to_json(ENC_RULES)
        file_format = sys.argv[3]
        compression = sys.argv[4]
        tb_name = sys.argv[5]
        target_format = sys.argv[6]
        delimiter = bytes(sys.argv[7], "utf-8").decode("unicode_escape")
        encoding = sys.argv[8]
        schema = decode_to_json(sys.argv[9])
        crypto_type = sys.argv[10]
        merge = sys.argv[11]

        sys.stdout.write(f"input_path_pattern : {input_path_pattern}, output_path: {output_path}, ciphers_keys : {ciphers_keys}, enc_rules : {enc_rules}\n")
        sys.stdout.write(f"file_format : {file_format}, target_format: {target_format}, compression : {compression}\n")
        sys.stdout.write(f"merge : {merge}\n")

        spark = SparkSession.builder.appName(f"encrypt_{tb_name}").getOrCreate()

        cipher_keys_broadcast = spark.sparkContext.broadcast(ciphers_keys)

        df = read_dataframe(spark, file_format, input_path_pattern, schema, delimiter, encoding)
        df = add_date_partition_from_path(df, input_path_pattern)

        sys.stdout.write(f"df.size : {df.count()} \n")
        if df.count() == 0:
            encrypted_df = spark.createDataFrame([], df.schema)

        else:
            encrypted_df = process_file(df, enc_rules, cipher_keys_broadcast, crypto_type)

        if merge == "true":
            encrypted_df = encrypted_df.coalesce(1)
            sys.stdout.write(f"merged to one file.\n")
        else:
            encrypted_df = encrypted_df.coalesce(39)

        sys.stdout.write(f"encrypted_df.size : {encrypted_df.count()}\n")
        if encrypted_df is None:
            raise ValueError("Error: Encrypted DataFrame is None. Skipping file processing.")

        output_path = os.getenv("OUTPUT_PATH", output_path)
        sys.stdout.write(f"output_path : {output_path}\n")

        write_dataframe(encrypted_df, target_format, compression, delimiter, output_path)

        if merge == "true":
            output_file_nm = get_filename_from_path(output_path, target_format)
            rename_single_part_in_hdfs(output_path, output_file_nm, spark)

    except Exception as e:
        error_msg = f"Error processing ORC file: {str(e)}"
        sys.stderr.write(f"{error_msg}\n")
        sys.stderr.write(traceback.format_exc())


if __name__ == "__main__":
    main()
