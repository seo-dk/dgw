import time
from kafka import KafkaConsumer
import logging
import re
from airflow.models import Variable

from commons_test.MetaInfoHook import KafkaSourceProtoInfo

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))

class KafkaMessageConsumer:

    def __init__(self, bootstrap_servers, kafka_proto_into: KafkaSourceProtoInfo):
        self.batch_size = 5000
        self.topic_name = kafka_proto_into.topicname
        self.consuming_timeout = kafka_proto_into.consuming_timeout
        self.filter_pattern = kafka_proto_into.filter_pattern

        logger.info("setting kafka config...")
        self.consumer = KafkaConsumer(
            self.topic_name,
            bootstrap_servers=bootstrap_servers,
            group_id=f"DATAGW_TEST_{self.topic_name}_CONSUMER_GROUP",
            auto_offset_reset="earliest",
            enable_auto_commit=True
        )

    def start(self):
        try:
            results = []
            last_message_time = time.time()

            while True:
                records = self.consumer.poll(timeout_ms=2000)

                if not records:
                    if time.time() - last_message_time > self.consuming_timeout:
                        logger.info(f"No messages received for {self.consuming_timeout} seconds. Exiting...")
                        break

                else:
                    for messages in records.values():
                        for message in messages:
                            value = message.value.decode('utf-8')
                            if re.search(self.filter_pattern, value):
                                continue
                            results.append(value)

                            if len(results) >= self.batch_size:
                                yield results
                                results = []

                    last_message_time = time.time()

            if results:
                yield results
        except Exception as e:
            raise Exception(f"Error occurred while consuming messages from topic: {self.topic_name}") from e

    def _close_consumer(self):
        try:
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka consumer closed successfully.")
        except Exception as e:
            logger.exception("Error occurred while closing Kafka consumer.")

    def __del__(self):
        self._close_consumer()