import json
import logging
import random
from dataclasses import dataclass
from typing import Union
from dataclasses import dataclass, field
from typing import Dict, List

import mysql.connector
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from commons_test.Util import Util
from typing import Dict

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
    memory_usage: str = "25g"

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
        self.memory_usage = "25g"

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
class ProvideInterfaceProtoInfo:
    delimiter: str = None
    target_dir: str = None
    target_file: str = None
    target_format: str = None
    create_chk_file: bool = False
    merge: bool = False
    source_file: str = None
    rename_uploaded_file: bool = False
    chk_file_extension: str = "CHK"
    preserve_filename: bool = False
    add_partitions: bool = False
    use_origin_path: bool = False
    convert_to: str = None
    allow_provide: bool = False
    separate_path: bool = False
    proxy_setting: Dict[str, str] = None
    staging_dir: str = None
    use_raw_file: bool = False
    change_delimiter: bool = False
    crypto_rules: CryptoRules = None
    spark_server_info: SparkServerInfo = None
    source_file_pattern: str = None
    target_compression: str = None
    format: str = None
    source_compression: str = None
    header_skip: bool = False
    no_target_compression: bool = False
    has_string_null: bool = False,
    clear_path: bool = False
@dataclass
class ProvideInterfaceProtoInfoDistcp:
    delimiter: str = None
    local_dir: str = None
    target_dir: str = None
    skip_if_partition_exists: bool = False
    skip_verify_count: bool = False
    add_partitions: bool = False
    use_origin_path: bool = False
    convert_to: str = None
    allow_provide: bool = False
    clear_path: bool = False
    merge: bool = False
    staging_dir: str = None
    use_raw_file: bool = False
    change_delimiter: bool = False
    crypto_rules: CryptoRules = None
    spark_server_info: SparkServerInfo = None
    source_file_pattern: str = None
    target_compression: str = None
    format: str = None
    source_compression: str = None
    target_format: str = None
    header_skip: bool = False
    no_target_compression: bool = False
    has_string_null: bool = False


@dataclass
class ProvideTargetInfo:
    id: str = None
    ip: str = None
    pwd: str = None
    port: str = None
    use_ssh_key: bool = False


@dataclass
class ProvideInterface:
    interface_id: str = None
    system_cd: str = None
    target_id: str = None
    protocol_cd: str = None
    proto_info: str = None
    db: str = None
    tb: str = None
    proto_info_json: Union[ProvideInterfaceProtoInfo, ProvideInterfaceProtoInfoDistcp] = None


@dataclass
class ProvideTarget:
    target_id: str = None
    target_info: str = None
    target_info_json: ProvideTargetInfo = None

@dataclass
class SystemCode:
    system_cd: str = None
    system_nm: str = None

class ProvideMetaInfoHook:

    def __init__(self):
        self._initialize_db_connection()

    def _initialize_db_connection(self):
        meta_conn_id_string = Variable.get("meta_conn_ids", default_var="meta_db_1")
        meta_conn_ids = meta_conn_id_string.split(",")

        random.shuffle(meta_conn_ids)

        self.mydb = None
        for conn_id in meta_conn_ids:
            try:
                conn = BaseHook.get_connection(conn_id)
                logger.info("Connecting to : %s", conn.conn_id)
                self.mydb = self.connect_to_mysql(conn)
                if self.mydb.is_connected() or self.mydb is not None:
                    self.cursor = self.mydb.cursor()
                    logger.info("connect success, connection id : %s", conn_id)
                    break
            except Exception as e:
                logger.warn("connection failed...", e)
                logger.info("trying another one")
                continue
        if not self.mydb.is_connected() or self.mydb is None:
            logger.error("Mysql connection all failed...")
            raise AirflowException("MetaInfoHook : DB connection failed", str(e))

    def _ensure_db_connection(self):
        if self.mydb is None or not self.mydb.is_connected():
            logger.warning("DB connection is closed. Reconnecting...")
            self._initialize_db_connection()

    def _get_records(self, sql, params=None):
        self._ensure_db_connection()

        self.cursor.execute(sql, params)
        records = self.cursor.fetchall()
        return records

    def get_provide_interface(self, interface_id):
        sql = """SELECT interface_id, system_cd, target_id, protocol_cd, proto_info, db, tb FROM provide_interface WHERE interface_id = %s"""
        params = [interface_id]
        logger.info("SQL : %s", Util.format_query_for_log(sql, params))
        records = self._get_records(sql, params)

        if not records:
            raise ValueError(f"No records found for interface_id {interface_id}")

        provide_interface = ProvideInterface(*records[0])
        logger.info("provide_interface info: %s", provide_interface)
        provide_interface.proto_info_json = self.get_proto_info(provide_interface)
        return provide_interface

    def get_provide_interface_by_tb(self, db, tb):
        # test 용으로 use_yn = Y 조건 제거
        sql = """SELECT interface_id, system_cd, target_id, protocol_cd, proto_info, db, tb 
                FROM provide_interface 
                WHERE db = %s 
                and tb =%s
                """
        params = [db, tb]
        logger.info("SQL : %s", Util.format_query_for_log(sql, params))
        records = self._get_records(sql, params)

        provide_interfaces = []

        for row in records:
            provide_interface = ProvideInterface(*row)
            logger.info("provide_interface info: %s", provide_interface)
            provide_interface.proto_info_json = self.get_proto_info(provide_interface)
            provide_interfaces.append(provide_interface)

        if not provide_interfaces:
            logger.info("No provide interfaces found for database '%s' and table '%s'", db, tb)

        return provide_interfaces

    def get_proto_info(self, provide_interface):
        json_data = json.loads(provide_interface.proto_info)
        crypto_rules = json_data.get('crypto_rules', {})
        spark_server_info_data = json_data.get("spark_server_info", {})
        no_target_compression = False
        if not json_data.get("target_compression"):
            no_target_compression = True
        if provide_interface.protocol_cd == 'DISTCP':
            return ProvideInterfaceProtoInfoDistcp(
                delimiter=json_data.get('delimiter'),
                local_dir=json_data.get('local_dir'),
                target_dir=json_data.get('target_dir'),
                skip_if_partition_exists=json_data.get('skip_if_partition_exists', False),
                skip_verify_count=json_data.get('skip_verify_count', False),
                add_partitions=json_data.get('add_partitions', False),
                use_origin_path=json_data.get('use_origin_path', False),
                convert_to=json_data.get("convert_to"),
                allow_provide=json_data.get("allow_provide", False),
                clear_path=json_data.get("clear_path", False),
                merge=json_data.get('merge', False),
                staging_dir=json_data.get('staging_dir'),
                use_raw_file=json_data.get('use_raw_file_test'),
                change_delimiter = json_data.get('change_delimiter', False),
                crypto_rules=CryptoRules(**crypto_rules),
                spark_server_info=SparkServerInfo(**spark_server_info_data),
                source_file_pattern=json_data.get('source_file_pattern'),
                target_compression=json_data.get("target_compression", "zstd"),
                format=json_data.get('format', 'csv'),
                source_compression=json_data.get("source_compression"),
                target_format=json_data.get("target_format"),
                header_skip=json_data.get("header_skip", False),
                no_target_compression=no_target_compression,
                has_string_null=json_data.get('has_string_null', False)
            )
        else:
            return ProvideInterfaceProtoInfo(
                delimiter=json_data.get('delimiter'),
                target_dir=json_data.get('target_dir'),
                target_file=json_data.get('target_file'),
                target_format=json_data.get('target_format'),
                create_chk_file=json_data.get('create_chk_file', False),
                merge=json_data.get('merge', False),
                source_file=json_data.get('source_file'),
                rename_uploaded_file=json_data.get('rename_uploaded_file', False),
                chk_file_extension=json_data.get('chk_file_extension', "CHK"),
                preserve_filename=json_data.get('preserve_filename', False),
                add_partitions=json_data.get('add_partitions', False),
                use_origin_path=json_data.get('use_origin_path', False),
                convert_to=json_data.get("convert_to"),
                allow_provide=json_data.get("allow_provide", False),
                separate_path=json_data.get("separate_path", False),
                proxy_setting=json_data.get("proxy_setting"),
                staging_dir=json_data.get('staging_dir'),
                use_raw_file=json_data.get('use_raw_file_test'),
                change_delimiter=json_data.get('change_delimiter', False),
                crypto_rules=CryptoRules(**crypto_rules),
                spark_server_info=SparkServerInfo(**spark_server_info_data),
                source_file_pattern=json_data.get('source_file_pattern'),
                target_compression=json_data.get("target_compression", "zstd"),
                format=json_data.get('format', 'csv'),
                source_compression=json_data.get("source_compression"),
                header_skip=json_data.get('header_skip', False),
                no_target_compression=no_target_compression,
                has_string_null=json_data.get('has_string_null', False),
                clear_path=json_data.get('clear_path', False)
            )

    def get_provide_target(self, target_id) -> ProvideTarget:
        sql = """SELECT target_id, target_info FROM provide_target
                 WHERE target_id = %s"""
        params = [target_id]
        logger.info("SQL : %s", Util.format_query_for_log(sql, params))

        records = self._get_records(sql, params)

        if not records:
            raise ValueError(f"No records found for target_id {target_id}")

        provide_target = ProvideTarget(*records[0])
        self._set_target_info_json(provide_target)
        logger.info("provide_target: %s", provide_target)
        return provide_target

    def _set_target_info_json(self, provide_target):
        json_data = json.loads(provide_target.target_info)
        provide_target.target_info_json = ProvideTargetInfo(
            id=json_data.get('id'),
            ip=json_data.get('ip'),
            pwd=json_data.get('pass'),
            port=json_data.get('port'),
            use_ssh_key=json_data.get('use_ssh_key')
        )

    def get_system_nm(self, system_cd):
        sql = """SELECT system_cd, system_nm FROM system_code WHERE system_cd =  %s"""
        params = [system_cd]
        logger.info("SQL : %s", Util.format_query_for_log(sql, params))

        records = self._get_records(sql, params)

        if not records:
            raise ValueError(f"No records found for system_cd {system_cd}")

        system_code = SystemCode(*records[0])
        logger.info("system_code: %s", system_code)

        return system_code

    def dbSessionClose(self):
        self.cursor.close()
        self.mydb.close()
        logger.info("DB session closed")

    def connect_to_mysql(self, conn):

        logger.info(f"connection : {conn}")
        config = {
            'host': conn.host,
            'user': conn.login,
            'password': conn.password,
            'database': conn.schema,
            'port': conn.port,
            'connection_timeout': 30
        }

        logger.info(f"connection config : {config}")

        mydb = mysql.connector.connect(**config)
        return mydb

if __name__ == "__main__":
    logger.info("Test MetaInfoHook Start!!!")

    meta_info_hook = ProvideMetaInfoHook()
    interface_info = meta_info_hook.get_provide_interface('P-IAM-FTP-OD-0071')
    logger.info("Result: %s", interface_info)

    provide_target = meta_info_hook.get_provide_target('SVR-0006')
    logger.info("Result: %s", provide_target)
