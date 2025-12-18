from airflow import DAG
from airflow.operators.bash import BashOperator
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
    'warehouse_aggregation',
    default_args=default_args,
    description='Aggregates data from Clean Zone to Data Warehouse (PostgreSQL)',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['warehouse', 'spark', 'aggregation'],
) as dag:

    aggregate_task = BashOperator(
        task_id='aggregate_to_warehouse',
        bash_command="""
            spark-submit \
            --master spark://spark:7077 \
            --packages org.postgresql:postgresql:42.2.18 \
            /opt/airflow/transformation/aggregate_to_warehouse.py
        """
    )

    aggregate_task
