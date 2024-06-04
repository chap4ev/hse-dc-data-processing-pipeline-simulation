from kafka_consumer import run_kafka_consumer
from greenplum_writer import GreenplumDriver

if __name__ == "__main__":
    greenplum_writer = GreenplumDriver()
    run_kafka_consumer(greenplum_writer)