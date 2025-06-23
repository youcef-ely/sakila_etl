import os, sys
import sqlite3
import logging
import pandas as pd
import pretty_errors
from dotenv import load_dotenv
from sqlalchemy import create_engine


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl_customer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)



current_dir = os.path.dirname(os.path.abspath(__file__)) 
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')

load_dotenv(dotenv_path)

source_user = os.environ.get('MYSQL_SOURCE_USER')
source_password = os.environ.get('MYSQL_SOURCE_ROOT_PASSWORD')
source_port = os.environ.get('MYSQL_SOURCE_PORT')
source_database = os.environ.get('MYSQL_SOURCE_DATABASE')
source_host = os.environ.get('MYSQL_SOURCE_HOST')




MYSQL_WAREHOUSE_HOST = os.environ.get('MYSQL_WAREHOUSE_HOST')  
MYSQL_WAREHOUSE_DATABASE = os.environ.get('MYSQL_WAREHOUSE_DATABASE')
MYSQL_WAREHOUSE_USER = os.environ.get('MYSQL_WAREHOUSE_USER')
MYSQL_WAREHOUSE_PORT = os.environ.get('MYSQL_WAREHOUSE_PORT')
MYSQL_WAREHOUSE_PASSWORD = os.environ.get('MYSQL_WAREHOUSE_ROOT_PASSWORD')



def extract_tables() -> pd.DataFrame:
    try:
        db_connection_str = f'mysql+pymysql://{source_user}:{source_password}@{source_host}:{source_port}/{source_database}'
        db_connection = create_engine(db_connection_str)
        logging.info(f"{source_database} Database connection established successfully")

    except Exception as e:  # Fixed: proper exception handling
        logging.error(f"Database connection failed: {e}")
        raise

    customer_table = pd.read_sql("""
        SELECT customer_id, store_id, first_name, last_name, email, address_id
        FROM customer
    """, con=db_connection)

    customer_address_table = pd.read_sql("""
        SELECT address_id, city_id, district
        FROM address
    """, con=db_connection)

    customer_city_table = pd.read_sql("""
        SELECT city_id, city, country_id
        FROM city
    """, con=db_connection)

    customer_country_table = pd.read_sql("""
        SELECT country_id, country
        FROM country
    """, con=db_connection)

    full_address = customer_address_table.merge(customer_city_table, on='city_id', how='inner')
    full_address = full_address.merge(customer_country_table, on='country_id', how='inner')
    customer_table = customer_table.merge(full_address, on='address_id', how='inner')

    return customer_table




def transform_table(customer_table) -> pd.DataFrame:
    customer_table['full_name'] = customer_table["first_name"] + customer_table['last_name']
    customer_table.drop(['first_name', 'last_name', 'city_id', 'country_id', 'address_id'], axis=1, inplace=True)
    
    customer_table = customer_table.rename(columns={'customer_id': 'customer_key', 'district': 'state'})
    return customer_table




def load_dimension(data=None) -> None:

    DIM_NAME = 'dim_client'
    try:
        db_url = f'mysql+pymysql://{MYSQL_WAREHOUSE_USER}:{MYSQL_WAREHOUSE_PASSWORD}@{MYSQL_WAREHOUSE_HOST}:{MYSQL_WAREHOUSE_PORT}/{MYSQL_WAREHOUSE_DATABASE}'
        db_connection = create_engine(db_url)

        logging.info(f"{MYSQL_WAREHOUSE_DATABASE} Database connection for loading established successfully")
    except Exception as e:
        logging.error(f"Database connection for loading failed: {e}")
        raise

    try:
        data.to_sql(con=db_connection, name=DIM_NAME, if_exists='append', index=False)
        logging.info(f'Loading into {DIM_NAME} successfully!')
    except Exception as e:  # Fixed: proper exception handling
        logging.error(f"Loading failed: {e}")
        raise





raw_tables = extract_tables()
transformed_tables = transform_table(raw_tables)
load_dimension(transformed_tables)