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
            coalesce(data.after.customer_id, data.before.customer_id) as customer_id,
            coalesce(data.after.name, data.before.name) as name,
            coalesce(data.after.city, data.before.city) as city,
            coalesce(data.after.email, data.before.email) as email,
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
                customer_id,
                name,
                city,
                email,
                created_at,
                updated_at,
                event_time,
                op,
                is_active,
                row_number() over(partition by customer_id order by event_time desc) as rn
            from
                bronze_clean
        )
        select
            customer_id,
            name,
            city,
            email,
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