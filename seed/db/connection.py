import os
import psycopg2
from psycopg2.extensions import connection
from dotenv import load_dotenv

load_dotenv()

def get_connection() -> connection:
    """
        Establish and return connection to the PostgreSQL database.

        Returns:
            connection: An active psycopg2 connection object bound to the target 
                relational database.
    """
    return psycopg2.connect(
        host=os.getenv("db_host"),
        port=os.getenv("db_port"),
        dbname=os.getenv("db_name"),
        user=os.getenv("db_user"),
        password=os.getenv("db_password"),
    )
