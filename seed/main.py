from seed.consumers.kafka_consumer import KafkaCDCConsumer
from seed.processors.cdc_processor import CDCProcessor
#from writers.local_writer import LocalWriter
from seed.writers.s3 import S3Writer
from seed.config.settings import KafkaConfig


def main():
    config = KafkaConfig()
    consumer = KafkaCDCConsumer(config)
    processor = CDCProcessor()
    writer = S3Writer()

    print("Listening to topics:")
    for topic in config.topics:
        print(f"  - {topic}")

    try:
        while True:
            msg = consumer.poll()

            if msg is None:
                continue
            if msg.error():
                print(f"Kafka Error: {msg.error()}")
                continue

            try:
                event = processor.process(msg)
                writer.write(event)
                print(
                    f"[{event['entity'].upper()}] "
                    f"OP={event['op']} "
                    f"LSN={event['lsn']} "
                    f"Timestamp={event['event_time']}"
                )

            except Exception as e:
                print(
                    f"Error processing message "
                    f"from topic {msg.topic()}: {e}"
                )
    except KeyboardInterrupt:
        print("Stopping consumer...")

    finally:
        for entity in writer.buffers.keys():
            writer.flush(entity)
        consumer.close()


if __name__ == "__main__":
    main()