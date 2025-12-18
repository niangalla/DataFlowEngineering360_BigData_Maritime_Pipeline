from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os
import subprocess

# Configuration
WEATHER_SCRIPT_PATH = '/opt/airflow/utils/generate_future_weather.py'
PORT_SCRIPT_PATH = '/opt/airflow/collection/generation/generate_historical_realistic.py'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_script(script_path):
    """Executes a python script as a subprocess."""
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Script not found: {script_path}")
    
    result = subprocess.run(['python3', script_path], capture_output=True, text=True)
    
    print(f"STDOUT: {result.stdout}")
    print(f"STDERR: {result.stderr}")
    
    if result.returncode != 0:
        raise Exception(f"Script failed with return code {result.returncode}")

with DAG(
    'setup_historical_data',
    default_args=default_args,
    description='Generate synthetic historical data (Weather + Port) for system initialization',
    schedule_interval=None, # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    tags=['setup', 'generation', 'init'],
) as dag:

    t1 = PythonOperator(
        task_id='generate_weather',
        python_callable=run_script,
        op_kwargs={'script_path': WEATHER_SCRIPT_PATH},
    )

    t2 = PythonOperator(
        task_id='generate_port_traffic',
        python_callable=run_script,
        op_kwargs={'script_path': PORT_SCRIPT_PATH},
    )

    trigger_batch = TriggerDagRunOperator(
        task_id='trigger_batch_ingestion',
        trigger_dag_id='hdfs_batch_ingestion',
        wait_for_completion=False
    )

    # Parallel execution then trigger
    [t1, t2] >> trigger_batch
