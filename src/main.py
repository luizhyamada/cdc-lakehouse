import argparse
import logging
from common.spark_session import create_session
from helpers.read_yaml import YamlReader
from jobs.bronze_pipeline import BronzePipeline
from src.jobs.silver_customer_pipeline import SilverPipeline
from jobs.silver_product_pipeline import SilverPipeline
from jobs.silver_order_pipeline import SilverPipeline

def main():
    """
        Parse command-line configurations and orchestrate the Lakehouse pipeline runtime.

        This function coordinates the application lifecycle:
            1. Parses command-line arguments to resolve config paths and execution target layers.
            2. Loads environmentalized configuration rules through `YamlReader`.
            3. Provisions a unified, optimized `SparkSession`.
            4. Initializes and triggers execution loops for selected layers (Bronze, Silver, or both).
            5. Enters a multi-threaded blocking state, awaiting termination signals for all active 
            Structured Streaming queries.

        Returns:
            None
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=False, default="src/configs/config.yaml")

    parser.add_argument("--layer", required=False, default="all", choices=["bronze", "silver", "all"])
    args = parser.parse_args()

    config_data = YamlReader.load_config(args.config)
    
    spark = create_session(f"lakehouse-cdc-pipeline-{args.layer}")
    active_queries = []

    if args.layer in ["bronze", "all"]:
        logging.info("Starting Bronze pipeline...")
        topics_list = config_data.get("topics", [])
        
        if topics_list:
            bronze_pipeline = BronzePipeline(spark_session=spark, topics_config=topics_list)
            bronze_queries = bronze_pipeline.run()
            active_queries.extend(bronze_queries)
        else:
            logging.warning("No topics mapped in YAML for the Bronze layer.")

    if args.layer in ["silver", "all"]:
        logging.info("Starting Silver pipeline...")
        tables_list = config_data.get("tables", [])
        
        if tables_list:
            silver_pipeline = SilverPipeline(spark_session=spark, tables_config=tables_list)
            silver_queries = silver_pipeline.run()
            active_queries.extend(silver_queries)
        else:
            logging.warning("No tables mapped in the YAML for the Silver layer.")

    if active_queries:
        logging.info(f"Wating {len(active_queries)} queries termination...")
        for query in active_queries:
            query.awaitTermination()
    else:
        logging.warning("No streaming queries were initiated. Job ending.")

if __name__ == "__main__":
    main()