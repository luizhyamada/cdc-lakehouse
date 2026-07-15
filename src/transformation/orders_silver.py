from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import from_json, col


def build_orders_silver(spark: SparkSession, df: DataFrame, schema) -> DataFrame:
    """
        Transform a raw Bronze micro-batch into a clean, deduplicated Silver DataFrame.
        
        Returns:
            DataFrame: A deduplicated Silver schema DataFrame, enriched with processing metadata.
    """
    df_parsed = df.withColumn("data", from_json(col("value").cast("string"), schema))
    df_parsed.createOrReplaceTempView("df")

    bronze_clean = spark.sql("""
        select
            coalesce(data.after.order_id, data.before.order_id) as order_id,
            coalesce(data.after.customer_id, data.before.customer_id) as customer_id,
            coalesce(data.after.product_id, data.before.product_id) as product_id,
            coalesce(data.after.quantity, data.before.quantity) as quantity,
            coalesce(data.after.unit_price, data.before.unit_price) as unit_price,
            coalesce(data.after.amount, data.before.amount) as amount,
            coalesce(data.after.status, data.before.status) as status,
            timestamp_micros(cast(coalesce(data.after.created_at, data.before.created_at) as bigint)) as created_at,
            timestamp_micros(cast(coalesce(data.after.updated_at, data.before.updated_at) as bigint)) as updated_at,
            timestamp_millis(cast(data.source.ts_ms as bigint)) as event_time,
            data.op,
            case
                when data.op = 'd' then false else true
            end as is_active
        from
            df
    """)
    bronze_clean.createOrReplaceTempView("bronze_clean")

    bronze_dedup = spark.sql("""
        with base as (
            select
                order_id,
                customer_id,
                product_id,
                quantity,
                unit_price,
                amount,
                status,
                created_at,
                updated_at,
                event_time,
                op,
                is_active,
                row_number() over(partition by order_id order by event_time desc) as rn
            from
                bronze_clean
        )
        select
            order_id,
            customer_id,
            product_id,
            quantity,
            unit_price,
            amount,
            status,
            created_at,
            updated_at,
            event_time,
            op,
            is_active,
            current_timestamp() as _processed_at,
            to_date(current_timestamp()) as _ingested_date
        from
            base
        where
            rn = 1
    """)

    return bronze_dedup