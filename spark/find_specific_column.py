import datetime
import os
import json
import sys
import traceback
from pyspark.sql import SparkSession
import re
from pyspark.sql.functions import col, explode, first, posexplode
from py4j.java_gateway import java_import
from pathlib import Path
from airflow_db import get_db_connection
import ast
import base64
import pandas as pd

env = os.environ.copy()
env['PATH'] = '/local/HADOOP/bin:' + env.get('PATH', '')


class SetPrivateDataInfo:
    def __init__(self, spark):
        self.spark = spark
    def get_hadoop_fs_and_file_list(self, spark, hdfs_path):
        java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
        java_import(spark._jvm, "org.apache.hadoop.fs.Path")

        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.FileSystem.get(hadoop_conf)
        path = spark._jvm.Path(hdfs_path)
        return fs, path


    def get_list_of_files(self, spark, hdfs_path, mode):

        fs,path = self.get_hadoop_fs_and_file_list(spark, hdfs_path)

        if not fs.exists(path):
            raise FileNotFoundError(f"{hdfs_path} does not exist")

        status_list = fs.listStatus(path)

        json_files = []

        for status in status_list:
            file_path = status.getPath()
            file_name = file_path.getName()
            if status.isFile() and file_name.endswith(mode):
                json_files.append(str(file_path.toString()))

        return json_files


    def get_match_hql_file_list(self, json_file_list, hql_file_list):
        json_bas_names = set(
            Path(p).stem for p in json_file_list
        )
        matched_hql_file_list = [
            hql_path for hql_path in hql_file_list
            if Path(hql_path).stem in json_bas_names
        ]
        return matched_hql_file_list


    def extract_columns_with_comment_keyword(self, hql_text, key_words_pattern, target_cols, db_tb_dict):
        pattern = re.compile(
            r"`(?P<col>\w+)`\s+\w+(?:\(\d+\))?\s+COMMENT\s+['\"](?P<comment>.*?)['\"]",
            re.IGNORECASE
        )
        for match in pattern.finditer(hql_text):
            col = match.group("col")
            comment = match.group("comment")

            matched_kw = next(
                (
                    kw for kw in key_words_pattern
                    if re.search(kw, comment.replace(" ", "")) or any(re.search(fr'(?:^|_){re.escape(target_column)}(?:_|$)', col)
                                                                      for target_column in target_cols)
                ), None
            )
            if matched_kw:
                if col not in db_tb_dict:
                    db_tb_dict[col] = comment


    def get_target_columns(self, spark, hql_file_paths, comment_keywords, target_columns):
        comment_dict = {}
        for hql_file_path in hql_file_paths:
            file_nm = os.path.basename(hql_file_path)
            db_tb_name, _ = os.path.splitext(file_nm)
            comment_dict[db_tb_name] = {}
            hql_rdd = spark.sparkContext.textFile(hql_file_path)
            hql_text = "\n".join(hql_rdd.collect())
            self.extract_columns_with_comment_keyword(hql_text, comment_keywords, target_columns, comment_dict[db_tb_name])
        return comment_dict


    def write_to_file(self, private_datas, private_data_path):
        with open(private_data_path, 'w', encoding='utf-8') as f:
            json.dump(private_datas, f, ensure_ascii=False, indent=2)
        sys.stdout.write(f"private data file created, file_name : {private_data_path}\n")


    def find_columns_in_schema(self, spark, json_file_paths, private_datas, db_tb_dict, target_columns):
        for path in json_file_paths:
            file_nm = os.path.basename(path)
            db_tb_name, _ = os.path.splitext(file_nm)
            db_tb_arr = db_tb_name.split(".")
            hadoop_path = f"hdfs://dataGWCluster/idcube_out/db={db_tb_arr[0]}/tb={db_tb_arr[1]}"
            private_datas[hadoop_path] = {}

            ## if hdfs://dataGWCluster/schema and hql exist
            self.check_columns_hql_and_schema(db_tb_dict, path, db_tb_name, spark, private_datas, hadoop_path)

            ## if only hdfs://dataGWCluster/schema
            self.check_columns_at_schema(target_columns, path, db_tb_name, spark, private_datas, hadoop_path)

            if not private_datas[hadoop_path]:
                del private_datas[hadoop_path]

    def check_columns_hql_and_schema(self, db_tb_dict, json_path, db_tb_name, spark, private_datas, hadoop_path):
        try:
            latitude_keyword = ["위도","경도","위경도", "경위도"]
            if db_tb_name in db_tb_dict:
                comment_dict = db_tb_dict[db_tb_name]
                df = spark.read.option("multiLine", True).json(json_path)
                if "fields" in df.columns:


                    field_names_df = (
                        df
                        .select(posexplode("fields").alias("index", "field"))
                        .select(col("index"), col("field.name").alias("name"), col("field.type").alias("type"))
                    )

                    rows = field_names_df.collect()

                    matched = [
                        {
                            "name": r["name"],
                            "index": r["index"],
                            "comment": comment_dict[r["name"]]
                        }
                        for r in rows
                        if (
                                r["name"] in comment_dict and (
                                    any(kw in comment_dict[r["name"]] for kw in latitude_keyword)
                                    or r["type"] == "string"
                                )
                        )
                    ]
                    if matched:
                        private_datas[hadoop_path]["columns"] = []
                        private_datas[hadoop_path]["columns"] = matched
        except Exception as e:
            sys.stderr.write(f"Error finding {db_tb_name} at json file {json_path}: {str(e)}\n")

    def check_columns_at_schema(self, target_columns, json_path, db_tb_name, spark, private_datas, hadoop_path):
        if not private_datas[hadoop_path]:
            try:
                df = spark.read.option("multiLine", True).json(json_path)
                if "fields" in df.columns:
                    field_names_df = (
                        df
                        .select(posexplode("fields").alias("index", "field"))
                        .filter(col("field.type") == "string")
                        .select(col("index"), col("field.name").alias("name"))
                    )

                    rows = field_names_df.collect()

                    patterns = [re.compile(fr'(?:^|_){re.escape(tc)}(?:_|$)') for tc in target_columns]

                    matched = [
                        {"name": r["name"], "index": r["index"]}
                        for r in rows
                        if any(p.search(r["name"]) for p in patterns)
                    ]
                    if matched:
                        private_datas[hadoop_path]["columns"] = []
                        private_datas[hadoop_path]["columns"] = matched
            except Exception as e:
                sys.stderr.write(f"Error finding {db_tb_name} at json file {json_path}: {str(e)}\n")


    def get_unique_columns(self, target_columns):
        uniq_target_columns = []
        seen_items = set()

        for col_dict in target_columns:
            item = tuple(col_dict.items())[0]
            if item not in seen_items:
                seen_items.add(item)
                uniq_target_columns.append(col_dict)
        return uniq_target_columns

    def make_private_data_info(self, private_data_json_path):
        hdfs_file_list_path = "./data/hdfs_file_list.dat"

        with open(private_data_json_path, 'r') as private_data_info_file:
            private_data_infos = json.load(private_data_info_file)

        with open(hdfs_file_list_path, 'r') as hdfs_file_list:
            file_paths = [line.strip() for line in hdfs_file_list if line.strip()]

        sorted_paths = sorted(file_paths)

        true_files = []
        for i, path in enumerate(sorted_paths):
            if path.endswith("/_SUCCESS"):
                continue
            if i + 1 < len(sorted_paths) and sorted_paths[i+1].startswith(path+ "/"):
                continue
            true_files.append(path)

        new_private_data_info = {}
        for base_path, value in private_data_infos.items():
            matched_files = [file_path for file_path in true_files if file_path.startswith(base_path)]
            if matched_files:
                new_private_data_info[matched_files[0]] = value

        with open(private_data_json_path, 'w') as private_data:
            json.dump(new_private_data_info, private_data, ensure_ascii=False, indent=2)

        sys.stdout.write(f"need to check {len(new_private_data_info)}\n")
        sys.stdout.write(f"file list at  {private_data_json_path}\n")

    def set_file_info_from_db(self, private_data_list_path):
        private_data_info = None
        with open(private_data_list_path, 'r') as file:
            private_data_info = json.load(file)

        for file_path in private_data_info:
            match = re.search(r'/db=([^/]+)/tb=([^/]+)', file_path)
            if match:
                hdfs_dir = f"/db={match.group(1)}/tb={match.group(2)}"
                db_result = self.get_data_info(hdfs_dir)
                if db_result:
                    parsed_result = tuple(
                        ast.literal_eval(val) if val is not None else None
                        for val in db_result
                    )

                    ext_idx = file_path.find(".")
                    if ext_idx != -1:
                        full_ext = file_path[ext_idx:]
                    else:
                        full_ext = ""

                    format, delimiter, encoding = parsed_result
                    if delimiter == "|^|" or delimiter == ",|'" or delimiter == '\",\"':
                        sys.stdout.write(f"changing {delimiter} to 036\n")
                        delimiter = "\036"

                    if not encoding:
                        encoding = 'UTF-8'

                    private_data_info[file_path]["format"] = format
                    private_data_info[file_path]["delimiter"] = delimiter
                    private_data_info[file_path]["encoding"] = encoding

                    match (full_ext, format, delimiter, encoding):
                        case (".dat", _, None, _):
                            print(f"check {file_path} delimiter")
                        case (".snappy", None, None, _):
                            print(f"check {file_path} format, delimiter")
                        case (".gz", None, _, _):
                            print(f"check {file_path} format")
                        case ("", None, _, _):
                            print(f"check {file_path} format")
                        case (".dat.lz4", _, None, _):
                            print(f"check {file_path} delimiter")
                        case (".lz4", None, None, _):
                            print(f"check {file_path} format, delimiter")
                        case (".dat.gz", None, None, _):
                            print(f"check {file_path} format, delimiter")
                        case (".dat", None, None, None):
                            print(f"check {file_path} format, delimiter, encoding")
                else:
                    print("No result from db")
                    continue
        with open(private_data_list_path, 'w') as file:
            json.dump(private_data_info, file, ensure_ascii=False, indent=2)

    def get_data_info(self, hdfs_dir):
        sql = """
            select MAX(proto_info -> '$.format') as 'format',
            MAX(ci_proto_info -> '$.delimiter') as 'delimiter',
            MAX(ci_proto_info -> '$.encode') as 'encode'
            from vw_collect_source_infos vcsi
            where hdfs_dir = %s
            """

        params = [hdfs_dir]
        with get_db_connection() as conn, conn.cursor() as cursor:
            cursor.execute(sql, params)
            result = cursor.fetchone()
            return result

    def start(self):
        try:
            start_time = datetime.datetime.now()

            target_columns = ["imsi", "imei", "mdn", "msisdn", "cdr", "enb_id", "cell_id", "svc_mgmt_num", "xdr", "cust_no"]
            # sys.stdout.write(f"target_column : {target_columns}\n")

            json_schema_path = "hdfs://dataGWCluster/schema"
            sys.stdout.write(f"json_schema_path : {json_schema_path}\n")
            hql_schema_path = "hdfs://dataGWCluster/schema/hql"
            sys.stdout.write(f"hql_schema_path : {hql_schema_path}\n")

            private_datas = {}
            json_file_paths = self.get_list_of_files(self.spark, json_schema_path, ".json")
            hql_file_paths = self.get_list_of_files(self.spark, hql_schema_path, ".hql")
            comment_keywords = [".*단말.*번호", ".*단말.*모델", ".*단말.*코드", "위도", "경도", "위경도", "경위도"]

            comment_dict = self.get_target_columns(self.spark, hql_file_paths, comment_keywords, target_columns)

            sys.stdout.write("-------unique columns-------\n")
            for db_tb_comment in comment_dict:
                sys.stdout.write(f"{db_tb_comment} : {comment_dict[db_tb_comment]}\n")
            sys.stdout.write("----------------------------\n")

            self.find_columns_in_schema(self.spark, json_file_paths, private_datas, comment_dict, target_columns)

            sys.stdout.write("-------result-------\n")
            for private_data in private_datas:
                sys.stdout.write(f"{private_data} : {private_datas[private_data]}\n")
            sys.stdout.write("----------------------------\n")

            sys.stdout.flush()

            private_data_path = "./data/private_data_info.json"

            self.write_to_file(private_datas, private_data_path)

            self.make_private_data_info(private_data_path)

            self.set_file_info_from_db(private_data_path)
            end_time = datetime.datetime.now()
            sys.stdout.write(f"setting private data info proccess time : {end_time - start_time}\n")
            return private_data_path
        except Exception as e:
            sys.stderr.write(f"Error processing ORC file: {str(e)}\n")
            sys.stderr.write(traceback.format_exc())

class ReadPrivateData:
    def __init__(self, spark, private_data_info_path):
        self.private_data_info_path = private_data_info_path
        self.spark = spark

    def is_base64_encoding(self, data, column_name, schema_name):
        base64_regex = re.compile(r'^[A-Za-z0-9+\/]*={0,2}$')
        imsi_flag = False
        if "imsi" in column_name or "imsi" in schema_name:
            data = data[2:]
            imsi_flag = True
        if imsi_flag:
            sys.stdout.write(f"checking {data} is encrypted\n")
        if not isinstance(data, str):
            if imsi_flag:
                sys.stdout.write("type not str\n")
            return False
        if len(data) % 4 != 0:
            if imsi_flag:
                sys.stdout.write("len not match\n")
            return False
        if not base64_regex.match(data):
            if imsi_flag:
                sys.stdout.write("regex not match\n")
            return False
        try:
            base64.b64decode(data, validate=True)
            return True
        except Exception:
            if imsi_flag:
                sys.stderr.write("Exception occur")
            return False

    def is_file(self, hdfs_path):
        java_import(self.spark._jvm, "org.apache.hadoop.fs.FileSystem")
        java_import(self.spark._jvm, "org.apache.hadoop.fs.Path")

        hadoop_conf = self.spark._jsc.hadoopConfiguration()
        fs = self.spark._jvm.FileSystem.get(hadoop_conf)
        path = self.spark._jvm.Path(hdfs_path)

        if fs.exists(path):
            if fs.isDirectory(path):
                sys.stdout.write(f"{path} is a directory\n")
                return False
        else:
            sys.stdout.write(f"{path} does not exist\n\n")
            return False

        return True

    def load_spark_data(self, private_data_path_infos):
        for private_data_path_info_path in private_data_path_infos:
            try:
                sys.stdout.write(f"reading {private_data_path_info_path}\n\n")
                private_data_info = private_data_path_infos[private_data_path_info_path]

                if not self.is_file(private_data_path_info_path):
                    continue

                ext_idx = private_data_path_info_path.find(".")
                if ext_idx != -1:
                    full_ext = private_data_path_info_path[ext_idx:]
                else:
                    full_ext = ""

                format = private_data_info["format"]
                if "dat" in full_ext or "DAT" in full_ext:
                    format = "csv"

                delimiter = private_data_info["delimiter"]

                if delimiter:
                    delimiter = bytes(private_data_info["delimiter"], "utf-8").decode("unicode_escape")

                df_reader = None
                if "parquet" in full_ext or format == "parquet":
                    df_reader = self.spark.read
                elif "orc" in full_ext or format == "orc":
                    df_reader = self.spark.read.format("orc")
                elif (any(ext in full_ext for ext in ("DAT", "dat", "csv", "txt")) or format in ("csv", "dat")) and delimiter:
                    df_reader = self.spark.read.format("csv").option('encoding', private_data_info["encoding"]).option('sep', delimiter)
                # elif ("txt" in full_ext) and delimiter:
                #     df_reader = self.spark.read.format("text").option('encoding', private_data_info["encoding"]).option('sep', delimiter)
                elif ("avro" in full_ext or format == "avro"):
                    df_reader = self.spark.read.format("avro")

                if df_reader:
                    if '[' in private_data_path_info_path or ']' in private_data_path_info_path:
                        yield df_reader.load(re.sub(r'([\[\]])', r'\\\1', private_data_path_info_path)), private_data_path_info_path, private_data_path_infos
                    yield df_reader.load(private_data_path_info_path), private_data_path_info_path, private_data_path_infos
                else:
                    sys.stderr.write(f"need to add another condition  : {private_data_path_info_path}\n\n")
                    yield (None, None, None)
            except Exception as e:
                sys.stderr.write(f"error during spark read : {private_data_path_info_path}\n\n")
                sys.stderr.write(traceback.format_exc())
                sys.stderr.write(f"error : {e}\n\n")

    def convert_json_to_csv(self):

        csv_file_path = "./data/plain_data.csv"
        with open(self.private_data_info_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        rows = []

        for file_path, content in data.items():
            columns = content.get("columns", [])
            fmt = content.get("format")
            delim = content.get("delimiter")
            enc = content.get("encoding")

            for col in columns:
                col_name = col.get("name")
                comment = col.get("comment")
                index = col.get("index")
                values = col.get("data", [])
                for value in values:
                    rows.append({
                        "file_path": file_path,
                        "column_name": col_name,
                        "index": index,
                        "value": value,
                        "comment": comment,
                        "format": "csv" if delim else fmt,
                        "delimiter": delim,
                        "encoding": enc
                    })

        df = pd.DataFrame(rows)
        df.to_csv(csv_file_path, index=False, encoding="utf-8-sig", escapechar='\\', sep='\036')

        sys.stdout.write(f'csv file write completed,  csv_file_path : {csv_file_path}\n')

    def read_private_data(self):

        if not os.path.exists(self.private_data_info_path):
            sys.stdout.write("There is no ./data/private_data_info.json")
            return

        private_data_path_infos = None
        with open(self.private_data_info_path, 'r') as file:
            private_data_path_infos = json.load(file)

        for data_frame, private_data_info_path, private_data_path_infos in self.load_spark_data(private_data_path_infos):
            if not data_frame:
                continue
            read_success = False
            try:
                cols_meta = private_data_path_infos[private_data_info_path]["columns"]
                cols_to_check = [data_frame.columns[m["index"]] for m in cols_meta]
                sample_df = (
                    data_frame
                    .select(*cols_to_check)
                    .limit(10000)
                    .cache()
                )
                sample_df.count()

                for meta in cols_meta:
                    col_name = data_frame.columns[meta['index']]

                    values = [
                        row[col_name]
                        for row in (
                            sample_df
                            .select(col(col_name))
                            .where(col(col_name).isNotNull())
                            .limit(6)
                            .collect()
                        )[1:6]
                    ]

                    meta['data'] = values
                    read_success = True

                    # is_encrypted = any(self.is_base64_encoding(v, col_name, meta['name']) for v in values)
                    # if is_encrypted:
                    #     sys.stdout.write(f"{private_data_info_path} {col_name}({meta['name']}) is encrypted\n")
                    # else:
                    #     meta['data'] = values
                    #     read_success = True

            except Exception as e:
                sys.stderr.write("\nerror selecting data with index, might check delimiter or data\n")
                sys.stderr.write(f"dataframe columns : {len(data_frame.columns)}\n\n")
                sys.stderr.write(traceback.format_exc())
                sys.stderr.write(f"error : {e}\n\n")

            if read_success:
                sys.stdout.write(f"{private_data_path_infos[private_data_info_path]}\nread finish\n\n")

        with open(self.private_data_info_path, 'w', encoding='utf-8') as f:
            json.dump(private_data_path_infos, f, ensure_ascii=False, indent=2)
        sys.stdout.write(f"private data info final file created, file_name : {self.private_data_info_path}\n")

        self.convert_json_to_csv()

    def start(self):
        start_time = datetime.datetime.now()
        try:
            self.read_private_data()
        except Exception as e:
            sys.stderr.write(f"Error reading private data file: {str(e)}\n")
            sys.stderr.write(traceback.format_exc())
        finally:
            end_time = datetime.datetime.now()
            sys.stdout.write(f"reading private data proccess time : {end_time - start_time}\n")

if __name__ == "__main__":
    spark = None
    try:
        start_time = datetime.datetime.now()
        sys.stdout.write(f"start spark!!!\n")
        spark = SparkSession.builder.appName(f"set private data").getOrCreate()

        set_private_data_info = SetPrivateDataInfo(spark)
        private_data_info_path = set_private_data_info.start()

        # private_data_info_path = "./data/private_data_info_v2_test.json"
        read_private_data = ReadPrivateData(spark, private_data_info_path)
        read_private_data.start()
        end_time = datetime.datetime.now()
        sys.stdout.write(f"total proccess time : {end_time - start_time}\n")
    except Exception as e:
        import sys
        sys.stderr.write(f"[Critical Error] : {e}\n")
        sys.stderr.flush()
    finally:
        if spark:
            spark.stop()
