from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow')
from collection.fetchers.fetch_historical_port import fetch_port_data, save_port_data

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

def fetch_and_process_wrapper(**context):
    """Wrapper to call the module function and pass data via XCom."""
    data = fetch_port_data()
    if not data:
        raise ValueError("No data fetched")
    return data

def save_to_databases_wrapper(**context):
    """Wrapper to call the module function with data from XCom."""
    ti = context['ti']
    data = ti.xcom_pull(task_ids='fetch_and_process_data')
    if not data:
        raise ValueError("No data received from fetch task")
    
    save_port_data(data, output_dir=DATA_DIR)

with DAG(
    'port_forecast_collection',
    default_args=default_args,
    description='Collect FUTURE port traffic forecast from Sinay API',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['collection', 'api', 'forecast'],
) as dag:

    t1 = PythonOperator(
        task_id='fetch_and_process_data',
        python_callable=fetch_and_process_wrapper,
    )

    t2 = PythonOperator(
        task_id='save_to_databases',
        python_callable=save_to_databases_wrapper,
    )

    t1 >> t2

    trigger_batch = TriggerDagRunOperator(
        task_id='trigger_batch_ingestion',
        trigger_dag_id='hdfs_batch_ingestion',
        wait_for_completion=False
    )

    t2 >> trigger_batch
