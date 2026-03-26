import logging
from kafka import KafkaConsumer
from airflow.models import Variable
import time
import re


logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, Variable.get(__name__, default_var="INFO"), logging.INFO))

class KafkaMessageConsumer:
    def __init__(self, bootstrap_servers, topic_name, result_file_path, filter_pattern, consuming_timeout):
        self.batch_size = 5000
        self.topic_name = topic_name
        self.consuming_timeout = consuming_timeout
        self.filter_pattern = filter_pattern
        self.result_file_path = result_file_path
        logger.info("setting kafka config...")
        self.consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            group_id = f"DATAGW_{topic_name}_CONSUMER_GROUP",
            auto_offset_reset = "latest",
            enable_auto_commit = True
        )

    def start(self):
        try:
            line_count = 0
            for results in self._consume():
                self._sink(results)
                line_count += len(results)
                logger.info("%d messages written to file.", len(results))

            logger.info("Kafka consuming completed for topic: %s. Total lines written: %d", self.topic_name, line_count)
        except Exception as e:
            raise Exception(f"Error occurred while consuming messages from topic: {self.topic_name}") from e
        finally:
            self._close_consumer()

    def _consume(self):
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

    def _sink(self, results):
        try:
            with open(self.result_file_path, "a") as result_file:
                result_file.writelines([line + "\n" for line in results])
            logger.info("Successfully written %d lines to %s", len(results), self.result_file_path)
        except Exception as e:
            logger.exception("Error while writing to file: %s", self.result_file_path)
            raise

    def _close_consumer(self):
        try:
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka consumer closed successfully.")
        except Exception as e:
            logger.exception("Error occurred while closing Kafka consumer.")

    def __del__(self):
        self._close_consumer()

if __name__ == "__main__":
    KafkaMessageConsumer("ipems_kafka_01:9092,ipems_kafka_02:9092,ipems_kafka_03:9092", "CM-EQUIP-MODEL", "/data/test/test.dat", re.compile("completed"), 10).start()
