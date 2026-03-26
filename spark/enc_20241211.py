#!/usr/bin/env python3
import os
import sys
import json
import base64
import traceback
import subprocess
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from StandaloneCipherWrapper import StandaloneCipherWrapper

env = os.environ.copy()
env['PATH'] = '/local/HADOOP/bin:' + env.get('PATH', '')

cipher_instances = {}


def get_cipher(enc_type, cipher_keys):
    global cipher_instances
    if enc_type not in cipher_instances:
        cipher_instances[enc_type] = StandaloneCipherWrapper(cipher_keys)
    return cipher_instances[enc_type]


def create_encrypt_udf(enc_type, cipher_keys):
    def encrypt(value):
        if value is None:
            return None
        try:
            cipher = get_cipher(enc_type, cipher_keys)
            return cipher.encrypt(enc_type, value)
        except Exception as e:
            sys.stderr.write(f"Error encrypting value: {value} with type: {enc_type}\n")
            return None

    return encrypt


def process_file(df, enc_rules, cipher_keys_broadcast):
    try:
        cipher_keys = cipher_keys_broadcast.value
        sys.stderr.write(f"Came to here \n")
        for rule in enc_rules:
            enc_type = rule["type"]
            for col_pos in rule["col_pos"]:
                column_name = df.columns[col_pos - 1]
                encrypt_udf = udf(create_encrypt_udf(enc_type, cipher_keys), StringType())
                df = df.withColumn(column_name, encrypt_udf(col(column_name)))
            sys.stderr.write(f"Came to here 2\n")

        return df
    except Exception as e:
        sys.stderr.write(f"Error during batch processing: {str(e)}\n")
        sys.stderr.write(traceback.format_exc())
        return None

def main():
    try:
        input_path = sys.argv[1]
        output_path = sys.argv[2]
        raw_ciphers_keys = sys.argv[3]
        raw_enc_rules = sys.argv[4]
        file_format = sys.argv[5]
        compression = sys.argv[6]
        tb_name = sys.argv[7]

        sys.stderr.write(f"file_format : {file_format}, compression : {compression}\n")

        spark = SparkSession.builder.appName(f"encrypt_{tb_name}").getOrCreate()

        json_compatible_enc_rules = raw_enc_rules.replace("'", '"')
        json_compatible_ciphers_keys = raw_ciphers_keys.replace("'", '"')

        json_enc_rule = json.loads(json_compatible_enc_rules)
        json_ciphers_keys = json.loads(json_compatible_ciphers_keys)

        cipher_keys_broadcast = spark.sparkContext.broadcast(json_ciphers_keys)

        df = spark.read.format(file_format).load(input_path)
        sys.stdout.write(f"df.size : {df.count()}")

        encrypted_df = process_file(df, json_enc_rule, cipher_keys_broadcast)
        encrypted_df = encrypted_df.coalesce(39)

        sys.stdout.write(f"encrypted_df.size : {encrypted_df.count()}")
        if encrypted_df is None:
            raise ValueError("Error: Encrypted DataFrame is None. Skipping file processing.")

        output_path = os.getenv("OUTPUT_PATH", output_path)
        encrypted_df.write.format(file_format).mode("overwrite").option("compression", compression).save(output_path)

    except Exception as e:
        sys.stderr.write(f"Error processing ORC file: {str(e)}\n")
        sys.stderr.write(traceback.format_exc())


if __name__ == "__main__":
    main()
