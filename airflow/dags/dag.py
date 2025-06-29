from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from tasks import etl_tasks  
from etl import RentalETL, DateETL, FilmETL, StoreETL, CustomerETL

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG("rental_etl_dag",
         default_args=default_args,
         schedule_interval="@daily",
         catchup=False) as dag:

    date_etl_task = PythonOperator(
        task_id="run_date_etl",
        python_callable=etl_tasks,
        op_kwargs={
            'etl_class': DateETL,
            'base_file_name': 'date'
        }
    )

    film_etl_task = PythonOperator(
        task_id="run_film_etl",
        python_callable=etl_tasks,
        op_kwargs={
            'etl_class': FilmETL,
            'base_file_name': 'film'
        }
    )

    store_etl_task = PythonOperator(
        task_id="run_store_etl",
        python_callable=etl_tasks,
        op_kwargs={
            'etl_class': StoreETL,
            'base_file_name': 'store'
        }
    )

    customer_etl_task = PythonOperator(
        task_id="run_customer_etl",
        python_callable=etl_tasks,
        op_kwargs={
            'etl_class': CustomerETL,
            'base_file_name': 'customer'
        }
    )

    rental_etl_task = PythonOperator(
        task_id="run_rental_etl",
        python_callable=etl_tasks,
        op_kwargs={
            'etl_class': RentalETL,
            'base_file_name': 'rental'
        }
    )

    [date_etl_task, film_etl_task, store_etl_task, customer_etl_task] >> rental_etl_task