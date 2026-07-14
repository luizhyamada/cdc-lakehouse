from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType, IntegerType, BooleanType

""""
This module defines the PySpark SQL schemas used for ingesting Change Data Capture 
(CDC) data into the Lakehouse. It mirrors a typical Debezium/Kafka Connect envelope 
structure, capturing the state of records 'before' and 'after' a database event, 
along with metadata.

Data Structures Defined:
    * `CUSTOMER_RECORD`: The baseline schema for the target customer table.
    * `SOURCE_SCHEMA`: Metadata from the source system (e.g., event timestamps).
    * `CUSTOMER_SCHEMA`: The full CDC envelope for customer events containing 
      the operational flag ('op') and state payloads.
    * `SCHEMAS_MAP`: A dictionary mapping table/topic names to their respective 
      CDC schemas for dynamic ingestion pipelines.

CDC Operation Types Reference ('op' field):
    * 'c' -> Create / Insert
    * 'u' -> Update
    * 'd' -> Delete
    * 'r' -> Read (Snapshot)
"""
SOURCE_SCHEMA = StructType([
    StructField("ts_ms", LongType())
])

def build_cdc_schema(record_schema: StructType) -> StructType:
    """
        Builds the Debezium CDC envelope for a table schema.

        Args:
            record_schema (StructType):
                Schema representing the table payload.

        Returns:
            StructType:
                Debezium CDC envelope containing:
                - before
                - after
                - source
                - op
    """
    return StructType([
        StructField("before", record_schema),
        StructField("after", record_schema),
        StructField("source", SOURCE_SCHEMA),
        StructField("op", StringType()),
    ])

CUSTOMER_RECORD = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("email", StringType(), True),
    StructField("created_at", LongType(), True),
    StructField("updated_at", LongType(), True),
])

PRODUCT_RECORD = StructType([
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("brand", StringType()),
    StructField("price", FloatType()),
    StructField("stock_quantity", IntegerType()),
    StructField("active", BooleanType()),
    StructField("created_at", LongType()),
    StructField("updated_at", LongType()),
])


SCHEMAS_MAP = {
    "customers": build_cdc_schema(CUSTOMER_RECORD),
    "products": build_cdc_schema(PRODUCT_RECORD)
}