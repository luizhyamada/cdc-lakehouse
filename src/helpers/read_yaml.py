import os
import yaml
import re
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class YamlReader:
    """
    Utility class to load YAML configuration files.
    """
    @staticmethod
    def load_config(file_path: str) -> dict:
        """
        Load and parse a YAML configuration file.

        Args:
            file_path (str): Path to the YAML configuration file.

        Returns:
            dict: Parsed YAML content. Returns an empty dictionary if the file is empty.

        Raises:
            FileNotFoundError: If the file does not exist.
            yaml.YAMLError: If the YAML is invalid.
        """
        if not os.path.exists(file_path):
            logging.error(f"YAML file not found: {file_path}")
            raise FileNotFoundError(f"File '{file_path}' does not exist.")

        try:
            with open(file_path, "r", encoding="utf-8") as file:
                config = yaml.safe_load(file) or {}

            logging.info(f"Configuration loaded from {file_path}")
            return config

        except yaml.YAMLError as e:
            logging.exception("Failed to parse YAML file.")
            raise

        except Exception as e:
            logging.exception("Unexpected error while reading YAML configuration.")
            raise