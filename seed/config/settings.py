from dataclasses import dataclass, field

@dataclass
class KafkaConfig:
    """
        Configuration profile for the Apache Kafka cluster ecosystem connections.

        Attributes:
            bootstrap_servers (str): Host and port coordinates to contact the Kafka cluster.
                Defaults to "kafka:9092".
            group_id (str): Unique string that identifies the consumer group protocol 
                for this streaming ecosystem. Defaults to "cdc-lakehouse".
            topics (List[str]): List of Kafka topic strings representing matching database tables 
                monitored by Change Data Capture.
    """
    bootstrap_servers = "kafka:9092"
    group_id = "cdc-lakehouse"
    topics: list[str] = field(
        default_factory=lambda: [
            "postgres.public.customers",
            "postgres.public.products",
            "postgres.public.orders"
        ]
    )