from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl_scripts.extract import extract_data
from etl_scripts.transform import transform_data, avg_weekly_sales_per_month
from etl_scripts.load import load_data, validation
import pandas as pd
from io import StringIO

store_data_path = "/mnt/d/Courses/Data Engineering/Walmart Pipeline/Data/grocery_sales.csv"
extra_data_path = "/mnt/d/Courses/Data Engineering/Walmart Pipeline/Data/extra_data.parquet"

full_data_path = "/mnt/d/Courses/Data Engineering/Walmart Pipeline/Data/full_data.csv"
agg_data_path = "/mnt/d/Courses/Data Engineering/Walmart Pipeline/Data/agg_data.csv"


def run_extract(**context):
    extracted = extract_data(store_data_path, extra_data_path)
    context['ti'].xcom_push(
        key='extract_data',
        value=extracted.to_json()
    )

def run_transform(**context):
    extracted = context['ti'].xcom_pull(
        key='extract_data',
        task_ids='extract'
    )
    extracted = pd.read_json(StringIO(extracted))
    transformed = transform_data(extracted)
    context['ti'].xcom_push(
        key='transformed_data',
        value=transformed.to_json()
    )

def run_analysis(**context):
    transformed = context['ti'].xcom_pull(
        key='transformed_data',
        task_ids='transform'
    )
    transformed = pd.read_json(StringIO(transformed))
    avg = avg_weekly_sales_per_month(transformed)
    context['ti'].xcom_push(
        key='avg_weekly_sales_per_month',
        value=avg.to_json()
    )

def run_load(**context):
    transformed = context['ti'].xcom_pull(
        key='transformed_data',
        task_ids='transform'
    )
    avg = context['ti'].xcom_pull(
        key='avg_weekly_sales_per_month',
        task_ids='analysis'
    )
    transformed = pd.read_json(StringIO(transformed))
    avg = pd.read_json(StringIO(avg))
    load_data(transformed, full_data_path, avg, agg_data_path)

default_args = {
    'owner': 'walmart',
    'retries':'1',
    'start_date': datetime(2026,5,22),
    'retry_delay': timedelta(minutes=2)
}

walmart_etl_dag = DAG(
    dag_id='walmart_etl_pipeline',
    default_args=default_args,
    schedule = '0 12 * * *',
    catchup=False
)

extract_task = PythonOperator(
    task_id = 'extract',
    python_callable=run_extract,
    dag=walmart_etl_dag
)

transform_task = PythonOperator(
    task_id = 'transform',
    python_callable=run_transform,
    dag=walmart_etl_dag
)

analysis_task = PythonOperator(
    task_id = 'analysis',
    python_callable=run_analysis,
    dag=walmart_etl_dag
)

load_task = PythonOperator(
    task_id = 'load',
    python_callable=run_load,
    dag=walmart_etl_dag
)

extract_task >> transform_task >> analysis_task >> load_task