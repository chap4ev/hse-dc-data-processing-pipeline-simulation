import logging
import json

from confluent_kafka import DeserializingConsumer, KafkaException, KafkaError
from configs import kafka_consumer_config
from greenplum_writer import GreenplumDriver


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)


def json_deserializer(bytes, SerializationContext):
    return json.loads(bytes.decode('utf-8'))


consumer_conf = {
    "value.deserializer": json_deserializer,
    **kafka_consumer_config["conf"]
}

consumer = DeserializingConsumer(consumer_conf)

def run_kafka_consumer(db: GreenplumDriver):
    try:
        consumer.subscribe([kafka_consumer_config["topic"]])

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logger.error(
                        'Topic %s [partition=%d] reached end at offset %d\n',
                        msg.topic(), msg.partition(), msg.offset(),
                    )
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                logger.info(
                    "Received kafka message(topic=%s, partition=%s, offset=[%d]): %s\n",
                    msg.topic(), msg.partition(), msg.offset(), msg.value(),
                )
                data = msg.value()

                # db.save_row(data)

    except KeyboardInterrupt:
        logger.warning('%% Aborted by user\n')

    finally:
        consumer.close()
        db.close()



