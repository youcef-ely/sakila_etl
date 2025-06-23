from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl.customer_etl import extract_task, transform_task, load_task

with DAG('customer_etl_pipeline',
         description='Extract, transform and load customer data from source to warehouse',
         schedule_interval='@daily',
         start_date=datetime(2025, 1, 1),
         catchup=False) as dag:
    
    extract_customer_data = PythonOperator(
        task_id='01_extract_customer_data',
        python_callable=extract_task,
        provide_context=True
    )
   
    transform_customer_data = PythonOperator(
        task_id='02_transform_customer_data',
        python_callable=transform_task,
        provide_context=True
    )
    
    load_customer_data = PythonOperator(
        task_id='03_load_customer_data',
        python_callable=load_task,
        provide_context=True
    )
    
    extract_customer_data >> transform_customer_data >> load_customer_data