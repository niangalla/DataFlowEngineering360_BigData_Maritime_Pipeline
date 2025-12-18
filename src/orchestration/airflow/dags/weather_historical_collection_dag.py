from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow')
from collection.fetchers.fetch_historical_weather import fetch_weather_data, save_weather_data

# Configuration
DATA_DIR = '/opt/airflow/data/source/from_apis'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_weather_wrapper(**context):
    """Wrapper to call the module function and pass data via XCom."""
    data = fetch_weather_data()
    if not data:
        raise ValueError("No data fetched")
    return data

def save_weather_wrapper(**context):
    """Wrapper to call the module function with data from XCom."""
    ti = context['ti']
    data = ti.xcom_pull(task_ids='fetch_weather')
    if not data:
        raise ValueError("No data received from fetch task")
    
    save_weather_data(data, output_dir=DATA_DIR)

with DAG(
    'weather_historical_collection',
    default_args=default_args,
    description='Fetch and ingest historical weather data (Native Python)',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['collection', 'weather', 'api', 'native'],
) as dag:

    t1 = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_weather_wrapper,
    )

    t2 = PythonOperator(
        task_id='save_weather',
        python_callable=save_weather_wrapper,
    )

    t1 >> t2

    trigger_batch = TriggerDagRunOperator(
        task_id='trigger_batch_ingestion',
        trigger_dag_id='hdfs_batch_ingestion',
        wait_for_completion=False
    )

    t2 >> trigger_batch
