import logging
from pyspark.sql import DataFrame
from common.delta_exists import write_or_create
from serve.dim_customers import build_dim_customers
from serve.dim_products import build_dim_products
from serve.fact_orders import build_fact_orders

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class GoldPipeline:
    """
    Pipeline to orchestrate the transformation and merge of Delta tables 
    from the Silver layer to the analytical Gold layer (Star Schema).
    """
    def __init__(self, spark_session, tables_config: list, gold_base_path: str):
        self.spark = spark_session
        self.tables_config = tables_config
        self.gold_base_path = gold_base_path.rstrip("/")

    def read_silver(self, silver_path: str) -> DataFrame:
        logging.info(f"Reading Silver data from path: {silver_path}")
        return (
            self.spark.readStream
            .format("delta")
            .load(silver_path)
        )

    def execute_merge_customers(self, batch_df: DataFrame, batch_id: int, target_path: str):
        if batch_df.isEmpty():
            logging.info(f"Customers batch {batch_id} is empty, skipping.")
            return

        logging.info(f"Processing Customers Gold batch ID {batch_id}")
        spark_batch_session = batch_df.sparkSession

        df_final = build_dim_customers(spark_batch_session, batch_df)
        df_final.createOrReplaceTempView("gold_customers_transformed")

        merge_sql = f"""
            MERGE INTO delta.`{target_path}` AS target
            USING gold_customers_transformed AS source
            ON target.customer_id = source.customer_id

            WHEN MATCHED THEN UPDATE SET
                target.customer_name = source.customer_name,
                target.city = source.city,
                target.email = source.email

            WHEN NOT MATCHED THEN INSERT *
        """

        write_or_create(
            df=df_final,
            spark=spark_batch_session,
            target_path=target_path,
            merge_sql=merge_sql,
        )

    def execute_merge_products(self, batch_df: DataFrame, batch_id: int, target_path: str):
        if batch_df.isEmpty():
            logging.info(f"Products batch {batch_id} is empty, skipping.")
            return

        logging.info(f"Processing Products Gold batch ID {batch_id}")
        spark_batch_session = batch_df.sparkSession

        df_final = build_dim_products(spark_batch_session, batch_df)
        df_final.createOrReplaceTempView("gold_products_transformed")

        merge_sql = f"""
            MERGE INTO delta.`{target_path}` AS target
            USING gold_products_transformed AS source
            ON target.product_id = source.product_id

            WHEN MATCHED THEN UPDATE SET
                target.product_name = source.product_name,
                target.category = source.category,
                target.brand = source.brand,
                target.current_price = source.current_price

            WHEN NOT MATCHED THEN INSERT *
        """

        write_or_create(
            df=df_final,
            spark=spark_batch_session,
            target_path=target_path,
            merge_sql=merge_sql,
        )

    def execute_merge_facts(self, batch_df: DataFrame, batch_id: int, target_path: str):
        if batch_df.isEmpty():
            logging.info(f"Orders batch {batch_id} is empty, skipping.")
            return

        logging.info(f"Processing Orders/Fact Gold batch ID {batch_id}")
        spark_batch_session = batch_df.sparkSession

        df_final = build_fact_orders(spark_batch_session, batch_df)
        df_final.createOrReplaceTempView("gold_facts_transformed")

        merge_sql = f"""
            MERGE INTO delta.`{target_path}` AS target
            USING gold_facts_transformed AS source
            ON target.order_id = source.order_id

            WHEN MATCHED 
            AND source.event_time > target.event_time
            THEN UPDATE SET
                target.customer_id = source.customer_id,
                target.product_id = source.product_id,
                target.status_id = source.status_id,
                target.date_key = source.date_key,
                target.quantity = source.quantity,
                target.unit_price = source.unit_price,
                target.amount = source.amount,
                target.event_time = source.event_time

            WHEN NOT MATCHED THEN INSERT *
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
            silver_path = config["silver_path"]
            
            if config["name"] == "customers":
                target_path = f"{self.gold_base_path}/dim_customers"
                checkpoint = f"{self.gold_base_path}/checkpoints/dim_customers"
                
                df_stream = self.read_silver(silver_path)
                query = (
                    df_stream.writeStream
                    .foreachBatch(lambda batch, batch_id, path=target_path: self.execute_merge_customers(batch, batch_id, path))
                    .option("checkpointLocation", checkpoint)
                    .start()
                )
                queries.append(query)

            elif config["name"] == "products":
                target_path = f"{self.gold_base_path}/dim_products"
                checkpoint = f"{self.gold_base_path}/checkpoints/dim_products"
                
                df_stream = self.read_silver(silver_path)
                query = (
                    df_stream.writeStream
                    .foreachBatch(lambda batch, batch_id, path=target_path: self.execute_merge_products(batch, batch_id, path))
                    .option("checkpointLocation", checkpoint)
                    .start()
                )
                queries.append(query)

            elif config["name"] == "orders":
                target_path = f"{self.gold_base_path}/fact_orders"
                checkpoint = f"{self.gold_base_path}/checkpoints/fact_orders"
                
                df_stream = self.read_silver(silver_path)
                query = (
                    df_stream.writeStream
                    .foreachBatch(lambda batch, batch_id, path=target_path: self.execute_merge_facts(batch, batch_id, path))
                    .option("checkpointLocation", checkpoint)
                    .start()
                )
                queries.append(query)

        return queries