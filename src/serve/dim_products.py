from pyspark.sql import DataFrame, SparkSession

def build_dim_products(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Transforms a clean Silver Products streaming micro-batch into a 
    Gold Dimension DataFrame.
    """
    df.createOrReplaceTempView("silver_products")

    gold_products = spark.sql("""
        SELECT
            product_id,
            product_name,
            category,
            brand,
            CAST(price AS DECIMAL(10,2)) AS current_price
        FROM
            silver_products
        WHERE
            is_active = true
    """)

    return gold_products