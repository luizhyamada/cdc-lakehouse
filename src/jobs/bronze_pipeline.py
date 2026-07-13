from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, to_date
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class BronzePipeline:
    """
        A pipeline class to manage the ingestion of raw Change Data Capture (CDC) 
        events from Apache Kafka into the Lakehouse Bronze layer using structured streaming.
    """
    def __init__(self, spark_session, topics_config: list):
        """
            Initialize the Bronze pipeline with configurations and the cluster session.

            Args:
                spark_session (SparkSession): The active Spark session.
                topics_config (List[Dict[str, Any]]): A list of dictionaries detailing the 
                    routing configurations.
        """
        self.spark = spark_session
        self.topics_config = topics_config
        self.bootstrap_servers = "kafka:9092"

    def read_stream(self, topic: str) -> DataFrame:
        """
            Read raw messages as a streaming DataFrame from a specific Kafka topic.
        """
        logging.info(f"Start Kafka topic read: {topic}")
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """
            Apply schema standardization and auditing metadata for the Bronze layer.
        """
        now = current_timestamp()
        return (
            df.select(
                col("key").cast("string").alias("key"),
                col("value").cast("string").alias("value"),
                col("topic").cast("string").alias("topic"),
                col("partition").cast("int").alias("kafka_partition"),
                col("offset").cast("long").alias("kafka_offset"),
                col("timestamp").cast("timestamp").alias("kafka_timestamp")
            )
            .withColumn("_ingested_at", now)
            .withColumn("_ingested_date", to_date(now))
        )

    def write_stream(self, df: DataFrame, output_path: str, checkpoint_path: str):
        """
            Write the streaming DataFrame to Amazon S3 in Delta Lake format.
        """
        logging.info(f"Writing streaming data to Delta path: {output_path}")
        
        return (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("path", output_path)
            .option("checkpointLocation", checkpoint_path)
            .partitionBy("_ingested_date")
            .start()
        )

    def run(self) -> list:
        """
            Orchestrate and start concurrent streaming queries for all configured topics.
        """
        queries = []
        
        if not self.topics_config:
            logging.warning("No topics were found in the configuration to process.")
            return queries

        for config in self.topics_config:
            df_raw = self.read_stream(config["topic"])
            
            df_transformed = self.transform(df_raw)
            
            query = self.write_stream(
                df=df_transformed,
                output_path=config["bronze_path"],
                checkpoint_path=config["checkpoint_bronze"]
            )
            queries.append(query)
            
        return queries