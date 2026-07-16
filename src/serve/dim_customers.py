from pyspark.sql import DataFrame, SparkSession

def build_dim_customers(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Transforms a clean Silver Customers streaming micro-batch into a 
    Gold Dimension DataFrame.
    """
    df.createOrReplaceTempView("silver_customers")

    gold_customers = spark.sql("""
        SELECT
            customer_id,
            name AS customer_name,
            city,
            email
        FROM
            silver_customers
        WHERE
            is_active = true
    """)

    return gold_customers