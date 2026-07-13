from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import from_json, col


def build_customers_silver(spark: SparkSession, df: DataFrame, schema) -> DataFrame:
    """
        Transform a raw Bronze micro-batch into a clean, deduplicated Silver DataFrame.
        
        Returns:
            DataFrame: A deduplicated Silver schema DataFrame, enriched with processing metadata.
    """
    df_parsed = df.withColumn("data", from_json(col("value").cast("string"), schema))
    df_parsed.createOrReplaceTempView("df")

    bronze_clean = spark.sql("""
        select
            coalesce(data.after.product_id, data.before.product_id) as product_id,
            coalesce(data.after.product_name, data.before.product_name) as product_name,
            coalesce(data.after.category, data.before.category) as category,
            coalesce(data.after.brand, data.before.brand) as brand,
            coalesce(data.after.price, data.before.price) as price,
            coalesce(data.after.stock_quantity, data.before.stock_quantity) as stock_quantity,
            coalesce(data.after.active, data.before.active) as is_active,
            timestamp_micros(cast(coalesce(data.after.created_at, data.before.created_at) as bigint)) as created_at,
            timestamp_micros(cast(coalesce(data.after.updated_at, data.before.updated_at) as bigint)) as updated_at,
            timestamp_millis(cast(data.source.ts_ms as bigint)) as event_time,
            data.op
        from
            df
    """)
    bronze_clean.createOrReplaceTempView("bronze_clean")

    bronze_dedup = spark.sql("""
        with base as (
            select
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
                row_number() over(partition by product_id order by event_time desc) as rn
            from
                bronze_clean
        )
        select
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
            current_timestamp() as _processed_at,
            to_date(current_timestamp()) as _ingested_date
        from
            base
        where
            rn = 1
    """)

    return bronze_dedup