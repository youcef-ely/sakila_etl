import os
import argparse
import pretty_errors

from models import Base
from dotenv import load_dotenv
from sqlalchemy import create_engine


dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)

MYSQL_WAREHOUSE_HOST = os.environ.get('MYSQL_WAREHOUSE_HOST')  
MYSQL_WAREHOUSE_DATABASE = os.environ.get('MYSQL_WAREHOUSE_DATABASE')
MYSQL_WAREHOUSE_USER = os.environ.get('MYSQL_WAREHOUSE_USER')
MYSQL_WAREHOUSE_PORT= os.environ.get('MYSQL_WAREHOUSE_PORT')
MYSQL_WAREHOUSE_PASSWORD = os.environ.get('MYSQL_WAREHOUSE_PASSWORD')


# Define your database URLs
DATABASES = {
    "datawarehouse": f'jdbc:mysql://{MYSQL_WAREHOUSE_HOST}:{MYSQL_WAREHOUSE_PORT}/{MYSQL_WAREHOUSE_DATABASE}',  
    "test": 'sqlite:///sakila_warehouse_test.db'
}

def create_datawarehouse(db_url: str):
    engine = create_engine(db_url, echo=True)
    Base.metadata.create_all(engine)
    print(f"Tables created in database: {db_url}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create database tables.")
    parser.add_argument(
        "--db",
        choices=DATABASES.keys(),
        default="datawarehouse",
        help="Specify which database to create tables in (default: datawarehouse)"
    )
    args = parser.parse_args()

    selected_db_url = DATABASES[args.db]
    create_datawarehouse(selected_db_url)


dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)



