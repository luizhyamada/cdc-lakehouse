from pyspark.sql import SparkSession

def create_session(app_name:str):
    """
        Initialize and configure a SparkSession with Delta Lake support.

        This function sets up a Spark session configured to connect to a standalone 
        Spark cluster master and integrates the necessary extensions and catalogs 
        for Delta Lake operations.
        Args:
            app_name (str): The name of the Spark application.

        Returns:
            SparkSession: A configured and active SparkSession instance.
    """
    spark = (
        SparkSession.builder
            .appName(app_name)
            .master("spark://spark-master:7077")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
    )
    
    return spark

