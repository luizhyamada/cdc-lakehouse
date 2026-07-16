import logging
from pyspark.sql import DataFrame, SparkSession
 
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
 
 
def table_exists(spark: SparkSession, path: str) -> bool:
    """
        Check if a valid Delta table exists at the specified path.

        This function utilizes `DeltaTable.isDeltaTable`, which validates the existence 
        of the table by checking for the presence of the `_delta_log` directory.

        Args:
            spark (SparkSession): The active Spark session.
            path (str): The storage path (HDFS, S3, ADLS, local) to be verified.

        Returns:
            bool: True if a valid Delta table exists at the path, False otherwise.
    """
    try:
        from delta.tables import DeltaTable
        return DeltaTable.isDeltaTable(spark, path)
    except Exception as e:
        logging.warning(f"Failed to verify Delta table existence at {path}: {e}")
        return False
 
 
def write_or_create(
        df: DataFrame, 
        spark: SparkSession, 
        target_path: str, 
        merge_sql: str,
        partition_by = None,
        temp_view_name: str = "micro_batch_updates"
) -> None:
    """
        Write data to a Delta target, creating it if missing or merging if it exists.

            If the target Delta table does not exist (e.g., during the first micro-batch), 
            it initializes the table using an 'overwrite' operation. For subsequent runs, 
            it executes the provided SQL `MERGE INTO` statement against the target path.

            Args:
                df (DataFrame): The incoming DataFrame containing the batch updates.
                spark (SparkSession): The active Spark session.
                target_path (str): The destination path of the Delta table.
                merge_sql (str): The fully resolved SQL string execution for the `MERGE INTO` command.
                partition_by (str/list, optional): Name of the column or columns to partition the Delta table.
                temp_view_name (str, optional): The name of the temporary view assigned 
                    to the incoming streaming DataFrame. Defaults to "micro_batch_updates".
                    
            Returns:
                None
    """
    df.createOrReplaceTempView(temp_view_name)
 
    if not table_exists(spark, target_path):
        logging.info(f"Delta table not found at {target_path}. Initializing table via overwrite.")
        
        writer = df.write.format("delta").mode("overwrite")
        
        if partition_by:
            logging.info(f"Partitioning target table by: {partition_by}")
            writer = writer.partitionBy(partition_by)
            
        writer.save(target_path)
        return
 
    logging.info(f"Delta table found at {target_path}. Executing MERGE operation.")
    spark.sql(merge_sql)
 
 
def get_delta_table(spark: SparkSession, path: str):
    """
        Instantiate and return a DeltaTable object for the specified path.

        Args:
            spark (SparkSession): The active Spark session.
            path (str): The storage path of the Delta table.

        Returns:
            Optional[DeltaTable]: A DeltaTable instance if valid metadata exists, 
                otherwise None.
    """
    from delta.tables import DeltaTable
 
    if not table_exists(spark, path):
        return None
    return DeltaTable.forPath(spark, path)