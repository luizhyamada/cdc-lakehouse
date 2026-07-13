from pyspark.sql.types import StructType, StructField, StringType, LongType

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

CUSTOMER_RECORD = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("email", StringType(), True),
    StructField("created_at", LongType(), True),
    StructField("updated_at", LongType(), True),
])

SOURCE_SCHEMA = StructType([
    StructField("ts_ms", LongType(), True)
])

CUSTOMER_SCHEMA = StructType([
    StructField("before", CUSTOMER_RECORD, True),
    StructField("after", CUSTOMER_RECORD, True),
    StructField("source", SOURCE_SCHEMA, True),
    StructField("op", StringType(), True),
])

SCHEMAS_MAP = {
    "customers": CUSTOMER_SCHEMA
}