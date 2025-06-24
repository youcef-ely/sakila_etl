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
        logging.FileHandler('logs/etl_store.log'),
        logging.StreamHandler()
    ]
)


logger = logging.getLogger(__name__)


source_db_config = get_config('sakila')
warehouse_db_config = get_config('sakila_dw')


def extract_film_data(engine) -> pd.DataFrame:
    film_df = pd.read_sql("""
        SELECT film_id, title, description, length
        FROM film
    """, con=engine)

    film_cat_df = pd.read_sql("""
        SELECT film_id, category_id
        FROM film_category
    """, con=engine)

    category_df = pd.read_sql("""
        SELECT category_id, name
        FROM category
    """, con=engine)


    merged_df = film_df.merge(film_cat_df, on='film_id').merge(category_df, on='category_id')
    return merged_df

# =======================
# Transform
# =======================
def transform_film_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transforms customer data for the data warehouse schema."""
    df.drop(['category_id'], axis=1, inplace=True)
    df.rename(columns={
        'name': 'category',
    }, inplace=True)
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
loaded_data = extract_film_data(engine)
transformed_data = transform_store_data(loaded_data)

print(transformed_data)