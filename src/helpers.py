import os, sys
import pretty_errors
from typing import Dict
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine


import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


current_dir = os.path.dirname(os.path.abspath(__file__))
dotenv_path = os.path.join(current_dir, '..', '.env')
load_dotenv(dotenv_path)



def get_config(db_name: str = 'sakila') -> Dict[str, str | None]:
    source_db_config = {
        "user": os.environ.get('MYSQL_SOURCE_USER'),
        "password": os.environ.get('MYSQL_SOURCE_ROOT_PASSWORD'),
        "host": os.environ.get('MYSQL_SOURCE_HOST'),
        "port": os.environ.get('MYSQL_SOURCE_PORT'),
        "database": os.environ.get('MYSQL_SOURCE_DATABASE'),
    }

    warehouse_db_config = {
        "user": os.environ.get('MYSQL_WAREHOUSE_USER'),
        "password": os.environ.get('MYSQL_WAREHOUSE_ROOT_PASSWORD'),
        "host": os.environ.get('MYSQL_WAREHOUSE_HOST'),
        "port": os.environ.get('MYSQL_WAREHOUSE_PORT'),
        "database": os.environ.get('MYSQL_WAREHOUSE_DATABASE'),
    }

    if db_name == 'sakila':
        return source_db_config
    elif db_name == 'sakila_dw':
        return warehouse_db_config
    else:
        raise ValueError(f"Unknown database name: {db_name}")



def create_db_engine(config: dict) -> Engine:
    try:
        url = f"mysql+pymysql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"
        engine = create_engine(url)
        logger.info(f"Connected to database `{config['database']}` successfully.")
        return engine
    except Exception as e:
        logger.error(f"Failed to connect to database `{config['database']}`: {e}")
        raise



def remove_file_safely(file_path):
    """Remove file if it exists, with error handling."""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Removed file: {file_path}")
    except Exception as e:
        logger.warning(f"Could not remove file {file_path}: {e}")