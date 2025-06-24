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
        logging.FileHandler('logs/etl_rental.log'),
        logging.StreamHandler()
    ]
)


logger = logging.getLogger(__name__)


source_db_config = get_config('sakila')
warehouse_db_config = get_config('sakila_dw')



def extract_rental_data(engine) -> pd.DataFrame:
    """Extract rental data with all necessary joins from source system"""
    logger.info("Extracting rental data from source system")
    
    query = """
        SELECT 
            r.rental_id,
            r.rental_date,
            r.customer_id,
            r.inventory_id,
            i.film_id,
            i.store_id,
            p.amount
        FROM rental r
        JOIN inventory i ON r.inventory_id = i.inventory_id
        JOIN payment p ON r.rental_id = p.rental_id
    """
    
    df = pd.read_sql(query, con=engine)
    logger.info(f"Extracted {len(df)} rental records")
    return df


def transform_rental_data(df: pd.DataFrame, dw_engine) -> pd.DataFrame:

    
    df['rental_date'] = pd.to_datetime(df['rental_date']).dt.date
    

    # Get date keys
    dim_date = pd.read_sql("SELECT date_key, full_date FROM dim_date", con=dw_engine)
    dim_date['full_date'] = pd.to_datetime(dim_date['full_date']).dt.date

    dim_customer = pd.read_sql("""
        SELECT customer_key, store_id as customer_store_id 
        FROM dim_client 
        ORDER BY customer_key
    """, con=dw_engine)
    
    # Get film keys - assuming dim_film has source_film_id or similar
    dim_film = pd.read_sql("""
        SELECT film_key, title 
        FROM dim_film 
        ORDER BY film_key
    """, con=dw_engine)
    
    # Get store keys
    dim_store = pd.read_sql("""
        SELECT store_key, address, city 
        FROM dim_store 
        ORDER BY store_key
    """, con=dw_engine)

    
    # Date lookup
    df = df.merge(dim_date, left_on='rental_date', right_on='full_date', how='left')

    df['customer_key'] = df['customer_id']  # This assumes 1:1 mapping
    
    # Film lookup - assuming film_key corresponds to film_id
    df['film_key'] = df['film_id']  # This assumes 1:1 mapping
    
    # Store lookup - assuming store_key corresponds to store_id
    df['store_key'] = df['store_id']  # This assumes 1:1 mapping
    
    # Select final columns for fact table
    fact_columns = ['customer_key', 'film_key', 'store_key', 'date_key', 'amount']
    fact_df = df[fact_columns].copy()
    
    # Remove records with missing dimension keys
    initial_count = len(fact_df)
    fact_df.dropna(subset=['customer_key', 'film_key', 'store_key', 'date_key'], inplace=True)
    final_count = len(fact_df)
    
    if initial_count != final_count:
        logger.warning(f"Dropped {initial_count - final_count} records due to missing dimension keys")
    
    # Ensure proper data types
    fact_df = fact_df.astype({
        'customer_key': 'int',
        'film_key': 'int',
        'store_key': 'int',
        'date_key': 'int',
        'amount': 'float'
    })
    
    logger.info(f"Transformation complete. Final record count: {len(fact_df)}")

    fact_df['rental_key'] = list(range(1, len(fact_df)+1))
    return fact_df


def load_dimension_table(df: pd.DataFrame, engine, table_name: str = 'fact_rental') -> None:
    """Loads transformed data into the data warehouse."""
    try:
        df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        logger.info(f"Data loaded into `{table_name}` successfully.")
    except Exception as e:
        logger.error(f"Failed to load data into `{table_name}`: {e}")
        raise


engine = create_db_engine(source_db_config)
rental_data = extract_rental_data(engine)
engine2 = create_db_engine(warehouse_db_config)
data = transform_rental_data(rental_data, engine2)

load_dimension_table(data, engine2)