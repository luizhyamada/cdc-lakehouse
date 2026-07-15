import logging
from pyspark.sql import DataFrame
from configs.schemas import SCHEMAS_MAP
from common.delta_exists import write_or_create
from transformation.products_silver import build_products_silver

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class SilverPipeline:
    """
        A pipeline class to manage the transformation and merging of CDC events 
        from the Bronze layer into deduplicated, state-aware Delta tables in the Silver layer.
    """
    def __init__(self, spark_session, tables_config: list):
        self.spark = spark_session
        self.tables_config = tables_config

    def read_bronze(self, bronze_path: str) -> DataFrame:
        logging.info(f"Reading Bronze data from path: {bronze_path}")
        return (
            self.spark.readStream
            .format("delta")
            .load(bronze_path)
        )

    def execute_merge(self, batch_df: DataFrame, batch_id: int, target_path: str, schema):
        if batch_df.isEmpty():
            logging.info(f"Batch {batch_id} is empty, skipping.")
            return

        logging.info(f"Processing batch ID {batch_id}")
        spark_batch_session = batch_df.sparkSession

        df_final = build_products_silver(spark_batch_session, batch_df, schema)

        df_final.createOrReplaceTempView("micro_batch_updates")

        merge_sql = f"""
                    MERGE INTO delta.`{target_path}` AS target
                    USING micro_batch_updates AS source
                    ON target.product_id = source.product_id

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
                        target.product_id = source.product_id,
                        target.product_name = source.product_name,
                        target.category = source.category,
                        target.brand = source.brand,
                        target.price = source.price,
                        target.stock_quantity = source.stock_quantity,
                        target.is_active = source.is_active,
                        target.created_at = source.created_at,
                        target.updated_at = source.updated_at,
                        target.event_time = source.event_time,
                        target.op = source.op,
                        target._processed_at = source._processed_at,
                        target._ingested_date = source._ingested_date

                    WHEN NOT MATCHED
                    AND source.op <> 'd'
                    THEN INSERT (
                        product_id,
                        product_name,
                        category,
                        brand,
                        price,
                        stock_quantity,
                        is_active,
                        created_at,
                        updated_at,
                        event_time,
                        op,
                        _processed_at,
                        _ingested_date
                    )
                    VALUES (
                        source.product_id,
                        source.product_name,
                        source.category,
                        source.brand,
                        source.price,
                        source.stock_quantity,
                        source.is_active,
                        source.created_at,
                        source.updated_at,
                        source.event_time,
                        source.op,
                        source._processed_at,
                        source._ingested_date
                    )
         """

        write_or_create(
            df=df_final,
            spark=spark_batch_session,
            target_path=target_path,
            merge_sql=merge_sql,
        )

    def run(self) -> list:
        queries = []
        for config in self.tables_config:
            if config["name"] == "products":
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