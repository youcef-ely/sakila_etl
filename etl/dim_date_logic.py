import os, sys
import logging
import pandas as pd
import pretty_errors


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from src.helpers import get_config, create_db_engine, remove_file_safely



logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl_date.log'),
        logging.StreamHandler()
    ]
)


logger = logging.getLogger(__name__)


source_db_config = get_config('sakila')
warehouse_db_config = get_config('sakila_dw')


def extract_date_data(engine) -> pd.DataFrame:
    date_df = pd.read_sql("""
        SELECT rental_date
        FROM rental
    """, con=engine)
    return date_df

# =======================
# Transform
# =======================
def transform_date_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transforms date data for the data warehouse schema."""
    # Convert to datetime first
    df['rental_date'] = pd.to_datetime(df['rental_date'])
    
    # Extract date components before converting to date (to avoid AttributeError)
    df['week'] = df['rental_date'].dt.isocalendar().week
    df['rental_month'] = df['rental_date'].dt.month
    
    # Convert to date after extracting components
    df['rental_date'] = df['rental_date'].dt.date
    
    # Remove duplicates
    df = df.drop_duplicates(subset='rental_date')
    
    # Generate date keys after deduplication
    df['date_key'] = range(1, len(df) + 1)
    df.reset_index(inplace=True)
    
    return df


# =======================
# Load
# =======================
def load_dimension_table(df: pd.DataFrame, engine, table_name: str = 'dim_store') -> None:
    """Loads transformed data into the data warehouse."""
    try:
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        logger.info(f"Data loaded into `{table_name}` successfully.")
    except Exception as e:
        logger.error(f"Failed to load data into `{table_name}`: {e}")
        raise


engine = create_db_engine(source_db_config)
loaded_data = extract_date_data(engine)
transformed_data = transform_date_data(loaded_data)

print(transformed_data)