import os, sys
import pretty_errors
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import all your ETL functions
from etl.customer_etl import extract_task as extract_customer, transform_task as transform_customer, load_task as load_customer
from etl.product_etl import extract_task as extract_product, transform_task as transform_product, load_task as load_product
from etl.store_etl import extract_task as extract_store, transform_task as transform_store, load_task as load_store
from etl.date_etl import extract_task as extract_date, transform_task as transform_date, load_task as load_date
from etl.sales_fact_etl import extract_task as extract_fact, transform_task as transform_fact, load_task as load_fact

with DAG('complete_dw_etl_pipeline',
         description='Complete data warehouse ETL pipeline',
         schedule_interval='@daily',
         start_date=datetime(2025, 1, 1),
         catchup=False) as dag:
    
    # Customer dimension
    extract_customer_data = PythonOperator(task_id='extract_customer', python_callable=extract_customer, provide_context=True)
    transform_customer_data = PythonOperator(task_id='transform_customer', python_callable=transform_customer, provide_context=True)
    load_customer_data = PythonOperator(task_id='load_customer', python_callable=load_customer, provide_context=True)
    
    # Product dimension
    extract_product_data = PythonOperator(task_id='extract_product', python_callable=extract_product, provide_context=True)
    transform_product_data = PythonOperator(task_id='transform_product', python_callable=transform_product, provide_context=True)
    load_product_data = PythonOperator(task_id='load_product', python_callable=load_product, provide_context=True)
    
    # Store dimension
    extract_store_data = PythonOperator(task_id='extract_store', python_callable=extract_store, provide_context=True)
    transform_store_data = PythonOperator(task_id='transform_store', python_callable=transform_store, provide_context=True)
    load_store_data = PythonOperator(task_id='load_store', python_callable=load_store, provide_context=True)
    
    # Date dimension
    extract_date_data = PythonOperator(task_id='extract_date', python_callable=extract_date, provide_context=True)
    transform_date_data = PythonOperator(task_id='transform_date', python_callable=transform_date, provide_context=True)
    load_date_data = PythonOperator(task_id='load_date', python_callable=load_date, provide_context=True)
    
    # Fact table
    extract_fact_data = PythonOperator(task_id='extract_fact', python_callable=extract_fact, provide_context=True)
    transform_fact_data = PythonOperator(task_id='transform_fact', python_callable=transform_fact, provide_context=True)
    load_fact_data = PythonOperator(task_id='load_fact', python_callable=load_fact, provide_context=True)
    
    # Dependencies - Each dimension ETL chain
    extract_customer_data >> transform_customer_data >> load_customer_data
    extract_product_data >> transform_product_data >> load_product_data
    extract_store_data >> transform_store_data >> load_store_data
    extract_date_data >> transform_date_data >> load_date_data
    
    # Fact table depends on all dimensions being loaded
    [load_customer_data, load_product_data, load_store_data, load_date_data] >> extract_fact_data
    extract_fact_data >> transform_fact_data >> load_fact_data