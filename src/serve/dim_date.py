import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

def build_dim_date(spark: SparkSession, start_year: int = 2020, end_year: int = 2030) -> DataFrame:
    days_count = (end_year - start_year + 1) * 365 + 3
    df = spark.range(0, days_count) \
              .withColumn("full_date", F.expr(f"date_add('{start_year}-01-01', cast(id as int))")) \
              .filter(F.year("full_date") <= end_year)
    
    return df.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        F.col("full_date"),
        F.year("full_date").alias("year"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.dayofmonth("full_date").alias("day_of_month"),
        F.date_format("full_date", "EEEE").alias("day_of_week"),
        F.quarter("full_date").alias("quarter")
    )