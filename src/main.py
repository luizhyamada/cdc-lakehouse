import argparse
import logging
from common.spark_session import create_session
from helpers.read_yaml import YamlReader
from jobs.bronze_pipeline import BronzePipeline
from jobs.silver_customer_pipeline import CustomersSilverPipeline
from jobs.silver_product_pipeline import ProductsSilverPipeline
from jobs.silver_order_pipeline import OrderSilverPipeline
from jobs.gold_pipeline import GoldPipeline

def main():
    """
    Parse command-line configurations and orchestrate the Lakehouse pipeline runtime.
    Allows targeting specific layers and individual tables to optimize local resource usage.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=False, default="src/configs/config.yaml")
    
    parser.add_argument(
        "--layer", 
        required=False, 
        default="all", 
        choices=["bronze", "silver", "gold", "all"]
    )

    parser.add_argument(
        "--table", 
        required=False, 
        default="all"
    )
    
    args = parser.parse_args()

    config_data = YamlReader.load_config(args.config)

    spark = create_session(f"lakehouse-cdc-{args.layer}-{args.table}")
    active_queries = []

    if args.layer in ["bronze", "all"]:
            logging.info("Starting Bronze pipeline...")
            topics_list = config_data.get("topics", [])
            
            if args.table != "all":
                topics_list = [t for t in topics_list if t.get("table") == args.table]

            if topics_list:
                bronze_pipeline = BronzePipeline(spark_session=spark, topics_config=topics_list)
                bronze_queries = bronze_pipeline.run()
                active_queries.extend(bronze_queries)
            else:
                logging.warning(f"No topics matched for the selection (table: {args.table}) in Bronze.")

    if args.layer in ["silver", "all"]:
            logging.info("Starting Silver pipeline...")
            tables_list = config_data.get("tables", [])
        
            if args.table != "all":
                tables_list = [t for t in tables_list if t.get("name") == args.table]

            if tables_list:
                for table_cfg in tables_list:
                    table_name = table_cfg["name"]
                    logging.info(f"Initializing Silver Pipeline for table: {table_name}")
                    
                    if table_name == "customers":
                        pipeline = CustomersSilverPipeline(spark_session=spark, tables_config=[table_cfg])
                    elif table_name == "products":
                        pipeline = ProductsSilverPipeline(spark_session=spark, tables_config=[table_cfg])
                    elif table_name == "orders":
                        pipeline = OrderSilverPipeline(spark_session=spark, tables_config=[table_cfg])
                    else:
                        logging.warning(f"No custom Silver pipeline class mapped for table: {table_name}")
                        continue
                    
                    silver_queries = pipeline.run()
                    active_queries.extend(silver_queries)
            else:
                logging.warning(f"No tables matched for the selection (table: {args.table}) in Silver.")

    if args.layer in ["gold", "all"]:
            logging.info("Starting Gold (Serve) pipeline...")
            tables_list = config_data.get("tables", [])
            
            if args.table != "all":
                tables_list = [t for t in tables_list if t.get("name") == args.table]

            gold_config = config_data.get("gold", {})
            gold_base_path = gold_config.get("base_path", "s3a://cdc-lakehouse-dbz/gold")
            
            if tables_list:
                gold_pipeline = GoldPipeline(
                    spark_session=spark, 
                    tables_config=tables_list, 
                    gold_base_path=gold_base_path
                )
                gold_queries = gold_pipeline.run()
                active_queries.extend(gold_queries)
            else:
                logging.warning(f"No tables matched for the selection (table: {args.table}) in Gold.")

    if active_queries:
        logging.info(f"Wating {len(active_queries)} queries termination...")
        for query in active_queries:
            query.awaitTermination()
    else:
        logging.warning("No streaming queries were initiated. Job ending.")

if __name__ == "__main__":
    main()