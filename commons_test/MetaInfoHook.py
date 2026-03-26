import json
import logging
import random
from dataclasses import dataclass, field
from typing import Dict, List

import mysql.connector
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from commons.Util import Util

from mysql.connector.pooling import MySQLConnectionPool
from mysql.connector import Error

from contextlib import contextmanager

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))


@dataclass
class SparkServerInfo:
    ip: str = field(default=None)
    username: str = field(default=None, repr=False)
    password: str = field(default=None, repr=False)
    port: int = 22
    queue: str = "encrypt_queue"
    exec_file_path: str = "/local/SPARK/python/encrypt"
    hadoop_schema_path: str = "hdfs://dataGWCluster/schema"
    is_external: bool = False

    def __post_init__(self):
        if not self.ip or not self.username or not self.password:
            conn_id = "gw-vhnn-004"
            self._load_from_airflow_conn(conn_id)

    def _load_from_airflow_conn(self,conn_id:str):
        conn = BaseHook.get_connection(conn_id)

        try:
            extra = json.loads(conn.extra)
        except Exception:
            extra = {}

        self.ip = conn.host
        self.username = conn.login
        self.password = conn.password
        self.port = conn.port or 22
        self.queue = extra.get("queue", self.queue)
        self.exec_file_path = extra.get("exec_file_path", self.exec_file_path)
        self.hadoop_schema_path = extra.get("hadoop_schema_path", self.hadoop_schema_path)
        self.is_external = extra.get("is_external", self.is_external)

@dataclass
class ColumnCryptoInfo:
    type: str = None
    col_pos: List[int] = None


@dataclass
class CryptoRules:
    enabled: bool = False
    is_encrypted: bool = False
    store_raw_and_crypto: bool = False
    rules: Dict[str, List[ColumnCryptoInfo]] = field(default_factory=dict)

    def __post_init__(self):
        if self.rules:
            for key, value in self.rules.items():
                if isinstance(value, list):
                    self.rules[key] = [ColumnCryptoInfo(**item) if isinstance(item, dict) else item for item in value]


@dataclass
class DbconnInterfaceProtoInfo:
    id: str = None
    ip: str = None
    pwd: str = None
    rac: str = None
    sid: str = None
    port: str = None
    type: str = None
    dbname: str = None
    service: str = None
    local_dir: str = None
    tb_overwrite: bool = None


@dataclass
class KafkaInterfaceProtoInfo:
    bootstrap_servers: str = None


@dataclass
class FtpSftpInterfaceProtoInfo:
    ip: str = None
    port: str = None
    local_dir: str = None
    id: str = None
    pwd: str = None
    source_dir: str = None
    source_file_nm: str = None
    ftp_trans_mode: str = None
    delimiter: str = None
    noti_dir: str = None
    noti_file_nm: str = None
    encode: str = None
    format: str = None
    use_partitions: bool = True
    dir_extract_pattern: str = None
    schemaless: bool = False
    external_process: bool = False
    ramdisk_dir: str = None
    bulk_upload: bool = False
    file_prefix: str = None
    header_skip: bool = False
    use_ssh_key: bool = False
    distcp_as_file: bool = False
    proxy_setting: Dict[str, str] = None
    binding_ip: str = None
    expand_source_dir_pattern: bool = False
    prefix_filenm_with_dir: bool = False


@dataclass
class DistcpInterfaceProtoInfo:
    encode: str = None
    file_nm: str = None
    local_dir: str = None
    source_dir: str = None
    noti_file_nm: str = None
    rename_table: bool = False
    delimiter: str = None
    skip_failure: bool = False


@dataclass
class DbconnSourceProtoInfo:
    num_rows: str = None
    table_nm: str = None
    data_query: str = None
    count_query: str = None
    data_params: str = None
    count_params: str = None
    data_source_file_nm: str = None
    partitions: str = None
    convert_to: str = None
    target_compression: str = None
    merge: bool = False
    contain_quote: bool = False
    delete_after_provide: bool = False
    no_target_compression: bool = False
    noti_partition: List[Dict[str, str]] = field(default_factory=list)

@dataclass
class FtpSourceProtoInfo:
    file_nm: str = None
    partitions: List[Dict[str, str]] = field(default_factory=list)
    source_file_nm: str = None
    dir_nm: str = None
    target_time: str = None
    use_regex_filename: bool = False
    header_skip: bool = False,
    delete_columns: list = field(default_factory=list),
    file_prefix: str = None
    convert_to: str = None
    target_schema: Dict[str, List[Dict[str, str]]] = None
    target_compression: str = None
    merge: bool = False
    prevent_split: bool = False
    crypto_rules: CryptoRules = None
    delete_after_provide: bool = False
    no_target_compression: bool = False
    delete_source_after_download: bool = False
    noti_partition: List[Dict[str, str]] = field(default_factory=list)
    align_filenm_with_partition: bool = False


@dataclass
class DistcpSourceProtoInfo:
    source_dir: str = None
    file_nm: str = None
    source_file_nm: str = None
    format: str = None
    skip_verify_count: bool = False
    convert_to: str = None
    target_schema: Dict[str, List[Dict[str, str]]] = None
    target_compression: str = None
    merge: bool = False
    file_rename: bool = False
    clear_path: bool = False
    header_skip: bool = False
    has_header: bool = False
    source_file_pattern: str = None
    enc_memory_usage: str = None
    spark_server_info: SparkServerInfo = SparkServerInfo()
    crypto_rules: CryptoRules = None
    target_format: str = None
    source_compression: str = None
    no_partitions: bool = False
    clear_subdirs: bool = False
    delete_after_provide: bool = False
    no_target_compression: bool = False
    convert_in_spark:bool = False
    noti_partition: List[Dict[str, str]] = field(default_factory=list)
    partitions: List[Dict[str, str]] = field(default_factory=list)


@dataclass
class KafkaSourceProtoInfo:
    encode: str = None
    topicname: str = None
    filter_pattern: str = None
    consuming_timeout: int = 10
    delimiter: str = None
    target_compression: str = None
    partitions: List[Dict[str, str]] = field(default_factory=list)
    delete_after_provide: bool = False
    no_target_compression: bool = False
    noti_partition: List[Dict[str, str]] = field(default_factory=list)


@dataclass
class SourceInfo:
    interface_id: str = None
    collect_source_seq: int = None
    proto_info: str = None
    hdfs_dir: str = None
    file_nm: str = None
    noti_allow: bool = False
    # isMappedWithProvideTable: bool = False
    file_proto_info: FtpSourceProtoInfo = None
    db_proto_info: DbconnSourceProtoInfo = None
    distcp_proto_info: DistcpSourceProtoInfo = None
    kafka_proto_info: KafkaSourceProtoInfo = None


@dataclass
class InterfaceInfo:
    interface_id: str = None
    protocol_cd: str = None
    interface_cycle: str = None
    job_schedule_exp: str = None
    ip_net: str = None
    system_cd: str = None
    proto_info: str = None
    ftp_proto_info: FtpSftpInterfaceProtoInfo = None
    db_proto_info: DbconnInterfaceProtoInfo = None
    distcp_proto_info: DistcpInterfaceProtoInfo = None
    kafka_proto_info: KafkaInterfaceProtoInfo = None
    target_time: str = None
    start_time: str = None


@dataclass
class StatusCodeInfo:
    status_cd: int = None
    status_nm: str = None


@dataclass
class CollectProvideInfo:
    collect_delimiter: str = None
    collect_encode: str = None
    collect_interface_id: str = None
    collect_hdfs_dir: str = None
    provide_interface_id: str = None
    provide_delimiter: str = None
    provide_encode: str = None
    provide_target_dir: str = None
    provide_protocol_cd: str = None
    target_id: str = None
    provide_use_yn: str = None

    @property
    def get_collect_delimiter(self):
        delimiter = self.collect_delimiter.decode("utf-8")
        if delimiter.startswith('\\'):
            delimiter = delimiter.encode("utf-8").decode('unicode_escape')

        return delimiter

    @property
    def get_provide_delimiter(self):
        delimiter = self.provide_delimiter.decode("utf-8")
        if delimiter.startswith('\\'):
            delimiter = delimiter.encode("utf-8").decode('unicode_escape')

        return delimiter


class MetaInfoHook:

    def __init__(self, kafka_send, collect_history):

        self.kafka_send = kafka_send
        self.collect_history = collect_history
        self.mydb = None
        self.connection_pool = None
        self._initialize_connection_pool()

    def _initialize_connection_pool(self):
        meta_conn_id_string = Variable.get("meta_conn_ids", default_var="meta_db_1")
        meta_conn_ids = meta_conn_id_string.split(",")

        random.shuffle(meta_conn_ids)

        for conn_id in meta_conn_ids:
            try:
                conn = BaseHook.get_connection(conn_id)
                logger.info("Creating connection pool for: %s", conn.conn_id)
                self.connection_pool = MySQLConnectionPool(
                    pool_name="meta_pool",
                    pool_size=5,  # Pool size 설정
                    host=conn.host,
                    user=conn.login,
                    password=conn.password,
                    database=conn.schema,
                    port=conn.port,
                    connection_timeout=30
                )
                logger.info("Connection pool created successfully for connection ID: %s", conn_id)
                break
            except Error as e:
                logger.warning("Failed to create connection pool for %s, trying another. Error: %s", conn_id, e)
                continue

        if not self.connection_pool:
            logger.error("All MySQL connection pool creation attempts failed.")
            self.kafka_send.sendErrorKafka(self.collect_history, 7, True, "Cannot connect to Kafka Server")
            raise AirflowException("Failed to establish a connection pool. All provided connection IDs have been tried.")

    def _get_connection(self):
        try:
            if not self.connection_pool:
                self._initialize_connection_pool()

            connection = self.connection_pool.get_connection()
            if connection.is_connected():
                return connection
            else:
                raise AirflowException("Failed to get connection from the pool.")
        except Error as e:
            logger.exception("Failed to get connection from the pool: %s", e)
            raise

    @contextmanager
    def _get_cursor(self):
        connection = self._get_connection()
        cursor = None
        try:
            cursor = connection.cursor()
            yield cursor
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def _get_record(self, sql, params=None):
        try:
            with self._get_cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchone()
        except Error as e:
            logger.exception("Error executing query: %s with params: %s", sql, params)
            raise

    def _get_records(self, sql, params=None):
        try:
            with self._get_cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchall()
        except Error as e:
            logger.exception("Error executing query: %s with params: %s", sql, params)
            raise

    def _get_ftp_interface_proto_info(self, json_data):
        return FtpSftpInterfaceProtoInfo(ip=json_data['ip'], port=json_data['port'],
                                         local_dir=json_data['local_dir'], id=json_data['id'],
                                         # pwd가 DB에 없을 때 KeyError 방지를 위해 .get() 사용 (None 반환)
                                         pwd=json_data.get('pwd'), source_dir=json_data['source_dir'],
                                         source_file_nm=json_data['file_nm'],
                                         ftp_trans_mode=json_data.get('ftp_trans_mode'),
                                         delimiter=json_data['delimiter'], noti_dir=json_data['noti_dir'],
                                         noti_file_nm=json_data['noti_file_nm'],
                                         encode=json_data['encode'], format=json_data.get("format", "csv"),
                                         use_partitions=json_data.get('use_partitions', True), dir_extract_pattern=json_data.get('dir_extract_pattern'),
                                         schemaless=json_data.get('schemaless', False), external_process=json_data.get('external_process', False),
                                         ramdisk_dir=json_data.get('ramdisk_dir'),
                                         bulk_upload=json_data.get('bulk_upload', False),
                                         file_prefix=json_data.get('file_prefix'),
                                         header_skip=json_data.get('header_skip', False),
                                         # use_ssh_key를 DB에서 받아오도록 추가 (기본값: False)
                                         use_ssh_key=json_data.get('use_ssh_key', False),
                                         distcp_as_file=json_data.get('distcp_as_file', False),
                                         proxy_setting=json_data.get('proxy_setting'),
                                         binding_ip=json_data.get('binding_ip'),
                                         expand_source_dir_pattern=json_data.get('expand_source_dir_pattern', False),
                                         prefix_filenm_with_dir=json_data.get('prefix_filenm_with_dir', False))

    def _get_ftp_source_proto_info(self, json_data, source_file_nm):

        ##at here we need to make encryption class at here
        crypto_rules = json_data.get('crypto_rules', {})
        no_target_compression = False
        if not json_data.get("target_compression"):
            no_target_compression = True
        return FtpSourceProtoInfo(file_nm=json_data['file_nm'], partitions=json_data.get('partitions'),
                                  source_file_nm=source_file_nm, dir_nm=json_data.get('dir_nm'),
                                  use_regex_filename=json_data.get('use_regex_filename'),
                                  header_skip=json_data.get('header_skip', False),
                                  delete_columns=json_data.get("delete_columns"), file_prefix=json_data.get('file_prefix'),
                                  convert_to=json_data.get("convert_to"), target_schema=json_data.get('target_schema'),
                                  target_compression=json_data.get("target_compression", "zstd"), merge=json_data.get('merge'),
                                  prevent_split=json_data.get("prevent_split"), crypto_rules=CryptoRules(**crypto_rules),
                                  delete_after_provide=json_data.get("delete_after_provide", False),no_target_compression=no_target_compression,
                                  delete_source_after_download=json_data.get("delete_source_after_download", False),
                                  noti_partition=json_data.get("noti_partition"),
                                  align_filenm_with_partition=json_data.get("align_filenm_with_partition", False))

    def _get_dbconn_interfce_proto_info(self, json_data):
        return DbconnInterfaceProtoInfo(id=json_data['id'], ip=json_data['ip'], pwd=json_data['pwd'],
                                        rac=json_data.get('rac', None), sid=json_data.get('sid', None), port=json_data['port'], dbname=json_data['db_name'],
                                        type=json_data['db_type'],
                                        service=json_data.get('service', None), local_dir=json_data['local_dir'],
                                        tb_overwrite=json_data.get('tb_overwrite', False))

    def _get_dbconn_source_proto_info(self, json_data, source_file_nm):
        return DbconnSourceProtoInfo(num_rows=json_data['num_rows'], table_nm=json_data['table_nm'],
                                     data_query=json_data['data_query'], count_query=json_data['count_query'],
                                     data_params=json_data['data_params'], count_params=json_data['count_params'],
                                     data_source_file_nm=source_file_nm,
                                     partitions=json_data.get('partitions', None), convert_to=json_data.get('convert_to', None),
                                     target_compression=json_data.get("target_compression", "zstd"), merge=json_data.get('merge', False),
                                     contain_quote=json_data.get('contain_quote', False),
                                     noti_partition=json_data.get("noti_partition"))
        # convert_to=json_data.get("convert_to"), target_schema=json_data.get('target_schema'),
        # target_compression=json_data.get("target_compression", "zstd"),
        # merge=json_data.get('merge'))

    def _get_distcp_interface_proto_info(self, json_data):
        return DistcpInterfaceProtoInfo(encode=json_data['encode'], file_nm=json_data['file_nm'],
                                        local_dir=json_data['local_dir'], source_dir=json_data['source_dir'],
                                        noti_file_nm=json_data['noti_file_nm'], rename_table=json_data.get('rename_table'),
                                        delimiter=json_data.get('delimiter', '\036'), skip_failure=json_data.get('skip_failure', False))

    def _get_distcp_source_proto_info(self, json_data, source_file_nm):

        spark_server_info_data = json_data.get("spark_server_info", {})
        crypto_rules = json_data.get('crypto_rules', {})
        no_target_compression = False
        if not json_data.get("target_compression"):
            no_target_compression = True

        return DistcpSourceProtoInfo(file_nm=json_data['file_nm'], source_dir=json_data.get('source_dir'), source_file_nm=source_file_nm, format=json_data.get('format', 'csv'),
                                     skip_verify_count=json_data.get('skip_verify_count', False),
                                     convert_to=json_data.get("convert_to"), target_schema=json_data.get('target_schema'),
                                     target_compression=json_data.get("target_compression", "zstd"),
                                     merge=json_data.get('merge'), file_rename=json_data.get('file_rename', False),
                                     clear_path=json_data.get('clear_path', False), header_skip=json_data.get('header_skip', False),
                                     has_header=json_data.get('has_header', False), source_file_pattern=json_data.get('source_file_pattern'),
                                     enc_memory_usage=json_data.get('enc_memory_usage', '25g'), spark_server_info=SparkServerInfo(**spark_server_info_data),
                                     crypto_rules=CryptoRules(**crypto_rules), target_format=json_data.get("target_format"), source_compression=json_data.get("source_compression"),
                                     no_partitions=json_data.get('no_partitions'), clear_subdirs=json_data.get('clear_subdirs'), delete_after_provide=json_data.get("delete_after_provide", False),
                                     no_target_compression=no_target_compression, convert_in_spark=json_data.get('convert_in_spark',False),
                                     noti_partition=json_data.get("noti_partition"),
                                     partitions=json_data.get("partitions") or [])

    def _get_kafka_interface_proto_info(self, json_data):
        return KafkaInterfaceProtoInfo(bootstrap_servers=json_data.get('bootstrap_servers'))

    def _get_kafka_source_proto_info(self, json_data):
        return KafkaSourceProtoInfo(encode=json_data.get('encode'), topicname=json_data.get('topicname'),
                                    filter_pattern=json_data.get('filter_pattern'), consuming_timeout=json_data.get('consuming_timeout'), delimiter=json_data.get('delimiter'),
                                    target_compression=json_data.get('target_compression', "zstd"), partitions=json_data.get('partitions'),
                                    noti_partition=json_data.get("noti_partition"))

    def get_interface_info(self, interface_id) -> InterfaceInfo:
        try:
            sql = """select interface_id, protocol_cd, interface_cycle, job_schedule_exp, ip_net, system_cd, CONVERT(proto_info USING utf8) as proto_info
                    from collect.collect_interface
                    where interface_id  = %s"""
            params = [interface_id]
            logger.info("SQL : %s", Util.format_query_for_log(sql, params))

            self.interface_info = InterfaceInfo(*self._get_record(sql, params))
            json_data = json.loads(self.interface_info.proto_info)

            if self.interface_info.protocol_cd == 'FTP' or self.interface_info.protocol_cd == 'SFTP':
                self.interface_info.ftp_proto_info = self._get_ftp_interface_proto_info(json_data)
            elif self.interface_info.protocol_cd == 'DBCONN':
                self.interface_info.db_proto_info = self._get_dbconn_interfce_proto_info(json_data)
            elif self.interface_info.protocol_cd == 'DISTCP':
                self.interface_info.distcp_proto_info = self._get_distcp_interface_proto_info(json_data)
            elif self.interface_info.protocol_cd == 'KAFKA':
                self.interface_info.kafka_proto_info = self._get_kafka_interface_proto_info(json_data)
            return self.interface_info

        except Exception as e:
            logger.exception("exception")
            raise AirflowException("MetaInfoHook : SQL Query Error", str(e))

    def get_source_infos(self, inf, file_name=None, table_name=None, seq_num=None, dest_path=None) -> List[SourceInfo]:
        try:
            # test 위해 use_yn = Y 제거
            sql = """
            select cs.interface_id, cs.collect_source_seq, CONVERT(cs.proto_info USING utf8), cs.hdfs_dir, ci.proto_info->>'$.file_nm' as file_nm, noti_allow
             from collect.collect_source cs
             join collect.collect_interface ci 
             on cs.interface_id = ci.interface_id 
            where cs.interface_id = %s
            """

            params = [inf]

            if file_name:
                sql += " AND JSON_EXTRACT(cs.proto_info, '$.file_nm') = %s"
                params.append(file_name)

            if table_name:
                sql += " AND substring(upper(cs.hdfs_dir), position('tb=' IN cs.hdfs_dir) + 3) = %s"
                params.append(table_name)

            if seq_num:
                sql += " AND cs.collect_source_seq = %s"
                params.append(seq_num)

            if dest_path:
                # sql += " AND CONCAT(%s, '/') LIKE CONCAT('%%', cs.hdfs_dir, '/%%')"
                sql += " AND CONCAT(%s, '/') LIKE CONCAT('%%/idcube_out', cs.hdfs_dir, '/%%')"
                params.append(dest_path)

            source_infos = []
            logger.info("SQL : %s", Util.format_query_for_log(sql, params))
            records = self._get_records(sql, params)
            for row in records:
                source_info = SourceInfo(*row)

                logger.info("fileInfo : %s", source_info)
                json_data = json.loads(source_info.proto_info)
                if self.interface_info.protocol_cd == 'FTP' or self.interface_info.protocol_cd == 'SFTP':
                    source_info.file_proto_info = self._get_ftp_source_proto_info(json_data, source_info.file_nm)
                elif self.interface_info.protocol_cd == 'DBCONN':
                    source_info.db_proto_info = self._get_dbconn_source_proto_info(json_data, source_info.file_nm)
                elif self.interface_info.protocol_cd == 'DISTCP':
                    source_info.distcp_proto_info = self._get_distcp_source_proto_info(json_data, source_info.file_nm)
                elif self.interface_info.protocol_cd == 'KAFKA':
                    source_info.kafka_proto_info = self._get_kafka_source_proto_info(json_data)

                source_infos.append(source_info)
            return source_infos

        except Exception as e:
            logger.exception("exception")
            raise AirflowException("MetaInfoHook : SQL Query Error", str(e))

    def get_dbconn_source_file_info(self, inf, seq_num):
        try:
            # test 위해 use_yn = Y 제거
            sql = """
            select cs.interface_id, cs.collect_source_seq, CONVERT(cs.proto_info USING utf8), cs.hdfs_dir, ci.proto_info->>'$.file_nm' as file_nm, noti_allow
             from collect.collect_source cs
             join collect.collect_interface ci 
             on cs.interface_id = ci.interface_id 
            where cs.interface_id = %s AND cs.collect_source_seq = %s
            """
            params = [inf, seq_num]

            logger.info("SQL : %s", Util.format_query_for_log(sql, params))
            record = self._get_record(sql, params)
            if not record:
                logger.error(f'no result for {seq_num}')
                return None

            file_info = SourceInfo(*record)
            file_info.db_proto_info = self._get_dbconn_source_proto_info(json.loads(file_info.proto_info), file_info.file_nm)

            return file_info
        except Exception as e:
            logger.exception("failed to get dbconn source file info: %s", interface_info)
            raise

    def get_status_code(self):
        try:
            sql = "select status_cd,status_nm from collect.status_code sc"
            logger.info("SQL : %s", sql)
            status_cd_infos = []
            records = self._get_records(sql)
            for row in records:
                statusCd = StatusCodeInfo(*row)
                status_cd_infos.append(statusCd)
            return status_cd_infos
        except Exception as e:
            logger.exception("exception")
            raise AirflowException("MetaInfoHook : SQL Query Error", str(e))

    def get_collect_provide_infos(self, interface_id):
        try:
            sql = "SELECT * FROM vw_collect_provide_infos WHERE collect_interface_id = %s"

            params = [interface_id]
            logger.info("SQL : %s", Util.format_query_for_log(sql, params))

            records = self._get_records(sql, params)

            if not records:
                logger.warning("no records: empty collect_provide_infos")
                return []

            collect_provide_infos = []

            for record in records:
                collect_provide_info = CollectProvideInfo(*record)
                collect_provide_infos.append(collect_provide_info)

            return collect_provide_infos

        except Exception as e:
            logger.exception("exception")
            raise AirflowException("MetaInfoHook : SQL Query Error", str(e))

    def close_pool(self):
        if self.connection_pool:
            logger.info("Closing connection pool...")
            self.connection_pool = None

    def close_db_session(self):
        self.close_pool()


if __name__ == "__main__":
    logger.info("Test MetaInfoHook Start!!!")

    meta_info_hook = MetaInfoHook(kafka_send=None, collect_history=None)

    test_interface_id = 'C-CDS-SFTP-MI-0003'
    interface_info = meta_info_hook.get_interface_info(test_interface_id)
    logger.info("Result: %s", interface_info)

    test_filenm = 'sample_file_name'
    test_tablenm = 'sample_table_name'

    folder_info = meta_info_hook.get_source_infos(test_interface_id, test_filenm, test_tablenm)
    logger.info("Result: %s", folder_info)

    source_file_info = meta_info_hook.get_dbconn_source_file_info('C-TMS-DBCONN-MI-0002', 1)
    logger.info("Result: %s", source_file_info)

    interface_info = meta_info_hook.get_interface_info('C-TMS-DBCONN-MI-0002')
    source_file_info = meta_info_hook.get_dbconn_source_file_info('C-TMS-DBCONN-MI-0002', 42448)
    logger.info("Result: %s", source_file_info)

    status_code_info = meta_info_hook.get_status_code()
    logger.info("Result: %s", status_code_info)

    collect_provide_infos = meta_info_hook.get_collect_provide_infos('C-IAM-FTP-DD-0001')
    logger.info("Result: %s", collect_provide_infos)

    logger.info("Test MetaInfoHook End!!!")
