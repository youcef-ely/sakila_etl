import os 
import pretty_errors
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

# Load .env
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)


def get_spark_config():
    """
    Creates and returns a SparkSession configured with both MySQL and SQLite JDBC drivers.
    """
    # Paths to both JARs
    base_dir = os.path.dirname(__file__)
    root_dir = os.path.abspath(os.path.join(base_dir, '..'))
    mysql_jar = os.path.join(root_dir, 'jars', 'mysql-connector-java-8.0.33.jar')
    sqlite_jar = os.path.join(root_dir, 'jars', 'sqlite-jdbc-3.42.0.0.jar')
    
    # Join both JARs
    jars = f"{mysql_jar},{sqlite_jar}"
    
    # Spark config
    spark_config = SparkConf()
    spark_config.setAppName('ETL_job')
    spark_config.set("spark.jars", jars)
    spark_config.set("spark.driver.memory", "2g")
    spark_config.set("spark.executor.memory", "2g")

    spark_session = SparkSession.builder.config(conf=spark_config).getOrCreate()
    spark_session.sparkContext.setLogLevel("WARN")
    return spark_session



def get_database_config(db_type='source'):
    """
    Returns the connection configuration dictionary for different database types.
    """
    if db_type == 'source':
        return {
            "url": f"jdbc:mysql://{os.environ.get('MYSQL_SOURCE_HOST')}:{os.environ.get('MYSQL_SOURCE_PORT')}/{os.environ.get('MYSQL_SOURCE_DATABASE')}",
            "user": os.environ.get('MYSQL_SOURCE_USER'),
            "password": os.environ.get('MYSQL_SOURCE_PASSWORD'),
            "driver": "com.mysql.cj.jdbc.Driver"
        }

    elif db_type == 'datawarehouse':
        return {
            "url": f"jdbc:mysql://{os.environ.get('MYSQL_WAREHOUSE_HOST')}:{os.environ.get('MYSQL_WAREHOUSE_PORT')}/{os.environ.get('MYSQL_WAREHOUSE_DATABASE')}",
            "user": os.environ.get('MYSQL_WAREHOUSE_USER'),
            "password": os.environ.get('MYSQL_WAREHOUSE_PASSWORD'),
            "driver": "com.mysql.cj.jdbc.Driver"
        }

    elif db_type == 'datawarehouse_test':
        # Full path to SQLite test database
        sqlite_path = os.path.join(os.path.dirname(__file__), '..', 'sql', 'test', 'sakila_dw.db')
        return {
            "url": f"jdbc:sqlite:{sqlite_path}",
            "driver": "org.sqlite.JDBC"
        }

    else:
        raise ValueError(f"Unsupported db_type: {db_type}")
