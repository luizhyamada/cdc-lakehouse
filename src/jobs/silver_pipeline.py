import logging
from pyspark.sql import DataFrame
from configs.schemas import SCHEMAS_MAP
from common.delta_exists import write_or_create
from transformation.customers_silver import build_customers_silver

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class SilverPipeline:
    """
        A pipeline class to manage the transformation and merging of CDC events 
        from the Bronze layer into deduplicated, state-aware Delta tables in the Silver layer.

        This component implements a typical Lakehouse architecture pattern by reading 
        append-only raw stream partitions, deduping updates using business key logic, 
        and performing upserts/deletes via idempotent `MERGE INTO` execution routines.
    """
    def __init__(self, spark_session, tables_config: list):
        """
            Initialize the Silver pipeline with configuration matrices.

            Args:
                spark_session (SparkSession): The active cluster Spark session.
                tables_config (List[Dict[str, Any]]): A collection of layout mappings 
                    specifying 'name', 'schema_name', 'bronze_path', 'silver_path', 
                    and 'checkpoint_silver'.
        """
        self.spark = spark_session
        self.tables_config = tables_config

    def read_bronze(self, bronze_path: str) -> DataFrame:
            """
            Read data from the Bronze layer in Delta streaming format.

            Args:
                bronze_path (str): The storage URI where the Bronze Delta records reside.

            Returns:
                DataFrame: An unbounded streaming DataFrame containing raw historical partitions.
            """
            logging.info(f"Reading Bronze data from path: {bronze_path}")
            return (
                self.spark.readStream
                .format("delta")
                .load(bronze_path)
            )
    def execute_merge(self, batch_df: DataFrame, batch_id: int, target_path: str, schema):
        """
            Process an isolated structural micro-batch and perform the Delta `MERGE INTO` operation.

            This method acts as the micro-batch execution routine for `foreachBatch`. It filters out 
            empty event sets, invokes the secondary transformation module to unpack the JSON CDC payload, 
            and applies an idempotent SQL MERGE template to handle conditional inserts, updates, and soft-deletes 
            based on event sequencing (`event_time`).

            Args:
                batch_df (DataFrame): A static, bounded representation of the micro-batch partition dataframe.
                batch_id (int): Automatically assigned structural identifier tracking the streaming loop.
                target_path (str): The target storage destination for the output Silver Delta table.
                schema (StructType): The specific Spark baseline schema struct used to extract inner objects.

            Returns:
                None
        """

        if batch_df.isEmpty():
            logging.info(f"Batch {batch_id} is empty, skipping.")
            return

        logging.info(f"Processing batch ID {batch_id}")

        spark_batch_session = batch_df.sparkSession

        df_final = build_customers_silver(spark_batch_session, batch_df, schema)

        merge_sql = f"""
            MERGE INTO delta.`{target_path}` AS target
            USING micro_batch_updates AS source
            ON target._ingested_date = source._ingested_date
            AND target.customer_id = source.customer_id

            WHEN MATCHED
            AND source.event_time > target.event_time
            AND source.op = 'd'
            THEN UPDATE SET
                target.event_time = source.event_time,
                target.op = source.op,
                target.is_active = false,
                target._processed_at = source._processed_at

            WHEN MATCHED
            AND source.event_time > target.event_time
            AND source.op IN ('c', 'u', 'r')
            THEN UPDATE SET
                target.name = source.name,
                target.city = source.city,
                target.email = source.email,
                target.created_at = source.created_at,
                target.updated_at = source.updated_at,
                target.event_time = source.event_time,
                target.op = source.op,
                target.is_active = true,
                target._processed_at = source._processed_at

            WHEN NOT MATCHED
            AND source.op <> 'd'
            THEN INSERT (
                customer_id,
                name,
                city,
                email,
                created_at,
                updated_at,
                event_time,
                op,
                is_active,
                _processed_at
            )
            VALUES (
                source.customer_id,
                source.name,
                source.city,
                source.email,
                source.created_at,
                source.updated_at,
                source.event_time,
                source.op,
                true,
                source._processed_at
            )
        """

        write_or_create(
            df=df_final,
            spark=spark_batch_session,
            target_path=target_path,
            merge_sql=merge_sql,
        )

    def run(self) -> list:
        """
            Orchestrate the Silver processing framework for defined entity matrices.

            Matches table entries tagged under 'customers', resolves their respective 
            ingestion payload structural schemas, and sets up a `foreachBatch` writer pipeline loops.

            Returns:
                List[StreamingQuery]: A list of streaming handles deployed to track ongoing background micro-batches.
        """
        queries = []
        for config in self.tables_config:
            if config["name"] == "customers":
                schema = SCHEMAS_MAP.get(config["schema_name"])
                df_stream = self.read_bronze(config["bronze_path"])

                query = (
                    df_stream.writeStream
                    .foreachBatch(lambda batch, batch_id, cfg=config, sch=schema: self.execute_merge(batch, batch_id, cfg["silver_path"], sch))
                    .option("checkpointLocation", config["checkpoint_silver"])
                    .start()
                )
                queries.append(query)

        return queries