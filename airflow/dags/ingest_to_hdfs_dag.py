from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "batch_ingestion_hdfs",
    schedule="@daily",
    start_date=datetime(2025, 10, 28),
    catchup=False
) as dag:

    ingest = BashOperator(
        task_id="ingest_batch",
        bash_command="python3 /opt/airflow/dags/scripts/batch_ingestion.py"
    )

    ingest
