from pyspark.sql import DataFrame, SparkSession

def build_fact_orders(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Transforms a clean Silver Orders streaming micro-batch into a 
    Gold Fact DataFrame.
    """
    df.createOrReplaceTempView("silver_orders")

    gold_facts = spark.sql("""
        SELECT
            order_id,
            customer_id,
            product_id,
            status AS status_id,
            CAST(DATE_FORMAT(created_at, 'yyyyMMdd') AS INT) AS date_key,
            quantity,
            CAST(unit_price AS DECIMAL(10,2)) AS unit_price,
            CAST(amount AS DECIMAL(10,2)) AS amount,
            created_at,
            event_time
        FROM
            silver_orders
        WHERE
            is_active = true
    """)

    return gold_facts