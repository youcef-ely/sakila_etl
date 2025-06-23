import os
import pretty_errors

from models import Base
from dotenv import load_dotenv
from sqlalchemy import create_engine


# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)

# Environment variables for data warehouse
MYSQL_WAREHOUSE_HOST = os.environ.get('MYSQL_WAREHOUSE_HOST')  
MYSQL_WAREHOUSE_DATABASE = os.environ.get('MYSQL_WAREHOUSE_DATABASE')
MYSQL_WAREHOUSE_USER = os.environ.get('MYSQL_WAREHOUSE_USER')
MYSQL_WAREHOUSE_PORT = os.environ.get('MYSQL_WAREHOUSE_PORT')
MYSQL_WAREHOUSE_PASSWORD = os.environ.get('MYSQL_WAREHOUSE_ROOT_PASSWORD')

# Construct the SQLAlchemy database URL
db_url = f'mysql+pymysql://{MYSQL_WAREHOUSE_USER}:{MYSQL_WAREHOUSE_PASSWORD}@{MYSQL_WAREHOUSE_HOST}:{MYSQL_WAREHOUSE_PORT}/{MYSQL_WAREHOUSE_DATABASE}'

def recreate_datawarehouse(db_url: str):
    engine = create_engine(db_url, echo=True)

    # Drop all existing tables
    print(f"Dropping all tables in database: {MYSQL_WAREHOUSE_DATABASE}")
    Base.metadata.drop_all(engine)

    # Create tables again
    print(f"Creating all tables in database: {MYSQL_WAREHOUSE_DATABASE}")
    Base.metadata.create_all(engine)

    print(f"Schema refreshed in database: {db_url}")

if __name__ == "__main__":
    recreate_datawarehouse(db_url)
