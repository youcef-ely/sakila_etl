import os, sys
import pretty_errors
from dotenv import load_dotenv
from pyspark.sql.functions import concat_ws, col


current_dir = os.path.dirname(os.path.abspath(__file__))  # tests/
project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))  # root of project

sys.path.insert(0, project_root)

dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
load_dotenv(dotenv_path)


from src.spark_config import get_spark_config, get_database_config


spark = get_spark_config()
sakila_warehouse_test_db = get_database_config(os.environ.get('MYSQL_WAREHOUSE_DATABASE_TEST'))


def get_customer_table():
    """
    Retrieve and transform customer data from the source database.
    """
    spark = get_spark_config()
    
    # Fixed: Use correct database type parameter
    sakila_db_config = get_database_config('sakila')
    
    # Debug: Print connection details (remove in production)
    print(f"Connecting to: {sakila_db_config['url']}")
    print(f"User: {sakila_db_config['user']}")
    
    try:
        df = spark.read \
            .format("jdbc") \
            .option("url", sakila_db_config['url']) \
            .option("user", sakila_db_config['user']) \
            .option("password", sakila_db_config['password']) \
            .option("driver", sakila_db_config['driver']) \
            .option("dbtable", "customer") \
            .load()

        df = df.select(
            col('CUSTOMER_ID'),
            col('STORE_ID'),
            concat_ws(' ', col('FIRST_NAME'), col('LAST_NAME')).alias('full_name'),
            col('EMAIL'),
            col('CITY'),
            col('STATE')
        )
        
        return df
        
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise




df = get_customer_table()
df.show()