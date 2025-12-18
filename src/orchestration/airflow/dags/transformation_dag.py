from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'transformation_full',
    default_args=default_args,
    description='Transform Raw Data to Clean Data using PySpark',
    schedule_interval=None, # Triggered by batch ingestion
    start_date=days_ago(1),
    catchup=False,
    tags=['transformation', 'spark', 'clean'],
) as dag:

    clean_port_data = BashOperator(
        task_id='clean_port_data',
        bash_command='spark-submit --master spark://spark:7077 --name "PortTrafficCleaning" /opt/airflow/transformation/clean_port_data.py'
    )

    clean_weather_data = BashOperator(
        task_id='clean_weather_data',
        bash_command='spark-submit --master spark://spark:7077 --name "WeatherDataCleaning" /opt/airflow/transformation/clean_weather_data.py'
    )

    trigger_aggregation = TriggerDagRunOperator(
        task_id='trigger_aggregation',
        trigger_dag_id='warehouse_aggregation',
        wait_for_completion=False
    )

    # Run both cleaning tasks in parallel, then trigger aggregation
    [clean_port_data, clean_weather_data] >> trigger_aggregation
