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
    """Transforms film data for the data warehouse schema."""
    df.drop(['category_id'], axis=1, inplace=True)
    df.rename(columns={
        'name': 'category',
    }, inplace=True)

    df.rename(columns={'film_id': 'film_key'}, inplace=True)
    return df

# =======================
# Load
# =======================
def load_dimension_table(df: pd.DataFrame, engine, table_name: str = 'dim_film') -> None:
    """Loads transformed data into the data warehouse."""
    try:
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        logger.info(f"Data loaded into `{table_name}` successfully.")
    except Exception as e:
        logger.error(f"Failed to load data into `{table_name}`: {e}")
        raise


def extract_task(ti):
    engine = create_db_engine(source_db_config)
    file_path = '/opt/airflow/shared/film_data.parquet'
    try:
        df = extract_film_data(engine)
        df.to_parquet(file_path, index=False)
        ti.xcom_push(key='film_path', value=file_path)
    except Exception as e:
        remove_file_safely(file_path)  # Cleanup on failure
        raise
    finally:
        engine.dispose()

def transform_task(ti):
    file_path = ti.xcom_pull(task_ids='extract_task', key='film_path')
    
    try:
        df = pd.read_parquet(file_path)
        df_transformed = transform_film_data(df)
        out_path = '/opt/airflow/shared/film_transformed.parquet'
        df_transformed.to_parquet(out_path)
        ti.xcom_push(key='date_data_transformed', value=out_path)
    
    finally:
        # Clean up the input file after using it
        remove_file_safely(file_path)

def load_task(ti):
    file_path = ti.xcom_pull(task_ids='date_transform_task', key='date_data_transformed')
    engine = create_db_engine(warehouse_db_config)
    
    try:
        df = pd.read_parquet(file_path)
        load_dimension_table(df, engine)
    finally:
        engine.dispose()  # Close database connection
        # Clean up the file after loading to database
        remove_file_safely(file_path)

