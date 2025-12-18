from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os
import subprocess

# Configuration
SCRAPER_SCRIPT_PATH = '/opt/airflow/collection/scrapers/vesselfinder_scraper.py'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),  # Retry after 15 mins if blocked
}

def run_scraper_script(script_path):
    """Executes the scraper script."""
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Script not found: {script_path}")
    
    # Run scraper
    result = subprocess.run(['python3', script_path], capture_output=True, text=True)
    
    print(f"STDOUT: {result.stdout}")
    print(f"STDERR: {result.stderr}")
    
    if result.returncode != 0:
        raise Exception(f"Scraper failed with return code {result.returncode}")

with DAG(
    'scraped_data_collection',
    default_args=default_args,
    description='Collect real-time port traffic data via web scraping (VesselFinder)',
    schedule_interval='0 */6 * * *',  # Every 6 hours (00:00, 06:00, 12:00, 18:00)
    start_date=days_ago(1),
    catchup=False,
    tags=['collection', 'scraping', 'real-time'],
) as dag:

    scrape_task = PythonOperator(
        task_id='scrape_vesselfinder',
        python_callable=run_scraper_script,
        op_kwargs={'script_path': SCRAPER_SCRIPT_PATH},
    )

    # Trigger ingestion immediately after scraping
    trigger_ingestion = TriggerDagRunOperator(
        task_id='trigger_ingestion',
        trigger_dag_id='hdfs_batch_ingestion',
        wait_for_completion=False
    )

    scrape_task >> trigger_ingestion
