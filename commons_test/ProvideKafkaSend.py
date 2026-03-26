import json
import logging
from dataclasses import asdict
from datetime import datetime

from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from kafka import KafkaProducer

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO").upper(), logging.INFO))

KAFKA_SERVER = 'gw-kafka-002:9092'
TOPIC_NAME = "meta_datagw"

class ProvideKafkaSend:
    def __init__(self):
        self.producer = self.set_kafka_config()
        self.partition_count = len(self.producer.partitions_for(TOPIC_NAME))

    def send_to_kafka(self, provide_history):
        try:
            partitionNum = self.ulid_to_part(provide_history.id, self.partition_count)
            jsondata = json.dumps(asdict(provide_history))
            self.producer.send(TOPIC_NAME, jsondata, partition=partitionNum)
            logger.info("kafka message produced: %s", jsondata)
        except Exception as e:
            logger.exception("KafkaError : %s", e)

    def close(self):
        self.producer.close()

    def ulid_to_part(self, ulid_value, n):
        base32_chars = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
        last_four_chars = ulid_value[-4:]
        number = 0
        for char in last_four_chars:
            number = number * 32 + base32_chars.index(char)
        return number % n

    def set_kafka_config(self):
        kafka_info_str = Variable.get("kafka-conf-ids", default_var="gw-kafka-002")
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

