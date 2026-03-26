import json
import logging
from dataclasses import asdict
from datetime import datetime

from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from kafka import KafkaProducer

from commons.Util import Util

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))

KAFKA_SERVER = 'gw-kafka-002:9092'
TOPIC_NAME = "hdfs_load_completion"


class KafkaSend:
    def __init__(self):
        self.producer = self.set_kafka_config()
        self.partition_count = len(self.producer.partitions_for(TOPIC_NAME))

    def send_to_kafka(self, collect_history):
        try:
            partitionNum = self.ulid_to_part(collect_history.collect_hist_id, self.partition_count)
            json_data = json.dumps(asdict(collect_history))
            self.producer.send(TOPIC_NAME, json_data, partition=partitionNum)
            logger.debug("kafka message produced. collect_hist_id: %s", collect_history.collect_hist_id)
            logger.debug('kafka produced json_data: %s', json_data)
        except Exception as e:
            logger.exception("KafkaError : %s", e)

    def close(self):
        self.producer.close()

    def ulid_to_part(self, ulid_value, n):
        base32_chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        last_four_chars = ulid_value[-4:]
        number = 0
        for char in last_four_chars:
            number = number * 32 + base32_chars.index(char)
        return number % n

    def sendErrorKafka(self, collect_history, num, need_id, msg):
        if need_id is True:
            collect_history.server_info.collect_hist_id = collect_history.get_new_ulid()
        collect_history.server_info.status_cd = self.status_code[num].status_cd
        collect_history.server_info.err_msg = msg
        collect_history.server_info.ended_at = datetime.now().isoformat()
        self.send_to_kafka(collect_history.server_info)

    def send_error(self, collect_history, num, msg):
        collect_history.status_cd = self.status_code[num].status_cd
        collect_history.err_msg = msg
        collect_history.ended_at = datetime.now().isoformat()
        self.send_to_kafka(collect_history)

    def set_kafka_config(self):

        kafka_info_str = Variable.get("kafka-conf-ids", default_var="gw-kafka-002")
        kafka_confs = []
        kafka_confs = kafka_info_str.split(",")

        servers = []

        for kafka_conf_id in kafka_confs:
            kafka_conf = BaseHook.get_connection(kafka_conf_id)
            server_info = f"{kafka_conf.host}:{kafka_conf.port}"
            servers.append(server_info)

        logger.info("bootstrap servers : %s", servers)

        conn = BaseHook.get_connection('gw-kafka-002')

        connection_info = {
            'extra': json.loads(conn.extra)
        }

        api_version_str = connection_info['extra'].get('api_version')
        api_version_tuple = tuple(int(x) for x in api_version_str.split('.'))
        logger.info(f"api version : {api_version_tuple}")
        request_timeout_ms = int(connection_info['extra'].get('request_timeout_ms'))
        logger.info(f"request time out : {request_timeout_ms}")

        producer = KafkaProducer(
            bootstrap_servers=servers,
            acks=int(connection_info['extra'].get('acks')),
            security_protocol=connection_info['extra'].get('security_protocol'),
            sasl_mechanism=connection_info['extra'].get('sasl_mechanism'),
            sasl_plain_username=connection_info['extra'].get('sasl_plain_username'),
            sasl_plain_password=connection_info['extra'].get('sasl_plain_password'),
            api_version=api_version_tuple,
            request_timeout_ms=request_timeout_ms,
            value_serializer=lambda x: x.encode('utf-8')
        )

        return producer

    def set_status_code(self, status_code):
        self.status_code = status_code

    ## TPDO-653 START## - noti_partition을 collect_history에 적용 (empty 체크 및 계산)
    def apply_noti_partition(self, collect_history, source_info, started_time=None):
        if source_info is None:
            return
        noti_partition = self._get_noti_partition(source_info)
        if not noti_partition:
            return
        if any("empty" in p and p.get("empty") for p in noti_partition):
            collect_history.partitions = []
            logger.info("noti_partition: set empty partitions")
            return
        calculated_partitions = self._calculate_noti_partition(noti_partition, started_time)
        if calculated_partitions:
            collect_history.partitions = calculated_partitions
            logger.info("noti_partition: calculated partitions = %s", calculated_partitions)
    ## TPDO-653##

    ## TPDO-653##
    def _get_noti_partition(self, source_info):
        if source_info.file_proto_info:
            return source_info.file_proto_info.noti_partition
        elif source_info.db_proto_info:
            return source_info.db_proto_info.noti_partition
        elif source_info.distcp_proto_info:
            return source_info.distcp_proto_info.noti_partition
        elif source_info.kafka_proto_info:
            return source_info.kafka_proto_info.noti_partition
        return None
    ## TPDO-653##

    ## TPDO-653##
    def _calculate_noti_partition(self, noti_partition, started_time):
        if not started_time:
            started_time = datetime.now().strftime("%Y%m%d%H%M")
        else:
            started_time_str = str(started_time)
            if len(started_time_str) == 8:
                started_time = started_time_str + "0000"
            elif len(started_time_str) == 10:
                started_time = started_time_str + "00"
            elif len(started_time_str) != 12:
                logger.warning("Unexpected started_time format: %s, using current time", started_time)
                started_time = datetime.now().strftime("%Y%m%d%H%M")
        calculated = []
        for partition in noti_partition:
            key = list(partition.keys())[0]
            value = partition[key]
            if Util.get_time_pattern(value):
                formatted_value = Util.get_calculate_pattern(value, started_time)
                calculated.append({key: formatted_value})
            else:
                calculated.append({key: value})
        return calculated
    ## TPDO-653##