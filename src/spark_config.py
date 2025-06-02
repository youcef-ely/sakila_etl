import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import pretty_errors  

# Load environment variables from .env
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# Constants for JAR paths (mounted via Docker)
JARS_DIR = "/opt/spark-apps/jars"
MYSQL_JAR = os.path.join(JARS_DIR, "mysql-connector-j-8.0.33.jar")
SQLITE_JAR = os.path.join(JARS_DIR, "sqlite-jdbc-3.42.0.0.jar")


def get_spark_session(app_name='etl_job') -> SparkSession:
    """
    Creates and returns a SparkSession with MySQL and SQLite JDBC drivers.
    """
    jars = ",".join([MYSQL_JAR, SQLITE_JAR])

    conf = SparkConf() \
        .setAppName(app_name) \
        .set("spark.jars", jars) \
        .set("spark.driver.memory", "2g") \
        .set("spark.executor.memory", "2g")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_database_config(db_type='sakila') -> dict:
    """
    Returns JDBC configuration based on the selected database type.
    Supports MySQL source, MySQL warehouse, and SQLite test warehouse.
    """
    driver = os.getenv('MYSQL_DRIVER')

    db_env = {
        'sakila': {
            'host': os.getenv('MYSQL_SOURCE_HOST'),
            'port': os.getenv('MYSQL_SOURCE_PORT'),
            'database': os.getenv('MYSQL_SOURCE_DATABASE'),
            'user': os.getenv('MYSQL_SOURCE_USER'),
            'password': os.getenv('MYSQL_SOURCE_ROOT_PASSWORD'),
        },
        'sakila_warehouse': {
            'host': os.getenv('MYSQL_WAREHOUSE_HOST'),
            'port': os.getenv('MYSQL_WAREHOUSE_PORT'),
            'database': os.getenv('MYSQL_WAREHOUSE_DATABASE'),
            'user': os.getenv('MYSQL_WAREHOUSE_USER'),
            'password': os.getenv('MYSQL_WAREHOUSE_ROOT_PASSWORD'),
        }
    }

    if db_type in db_env:
        cfg = db_env[db_type]
        return {
            'url': f"jdbc:mysql://{cfg['host']}:{cfg['port']}/{cfg['database']}",
            'user': cfg['user'],
            'password': cfg['password'],
            'driver': driver
        }

    if db_type == 'sakila_warehouse_test':
        sqlite_path = os.path.join(os.path.dirname(__file__), '..', 'sql', 'test', 'sakila_warehouse_test.db')
        return {
            'url': f"jdbc:sqlite:{sqlite_path}",
            'driver': "org.sqlite.JDBC"
        }

    raise ValueError(f"Unsupported db_type: {db_type}")
