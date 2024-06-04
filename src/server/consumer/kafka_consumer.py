import logging
import json

from confluent_kafka import Consumer, KafkaException, KafkaError
from configs import kafka_consumer_config
from greenplum_writer import GreenplumDriver


consumer = Consumer(kafka_consumer_config)

logger = logging.getLogger(__name__)


def run_kafka_consumer(db: GreenplumDriver):
    try:
        consumer.subscribe(kafka_consumer_config["topic"])

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logger.error(
                        'Topic %s [partition=%d] reached end at offset %d\n' %
                        (msg.topic(), msg.partition(), msg.offset())
                    )
                elif msg.error():
                    raise KafkaException(msg.error())
                else:
                    logging.info(
                        "Received kafka message(topic=%s, partition=%s, offset=[%d]): %s\n" %
                        (msg.topic(), msg.partition(), msg.offset(), msg.value().decode("utf-8"))
                    )
                    data = json.loads(msg.value().decode('utf-8'))
                    db.save_row(data)

    except KeyboardInterrupt:
        logger.warning('%% Aborted by user\n')

    finally:
        consumer.close()
        db.close()



