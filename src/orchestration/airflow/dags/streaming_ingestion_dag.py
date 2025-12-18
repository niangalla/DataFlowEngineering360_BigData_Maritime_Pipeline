from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'streaming_ingestion',
    default_args=default_args,
    description='Run Kafka Producer and Spark Consumer in micro-batches',
    schedule_interval= timedelta(minutes=5),
    # '* /5 * * *', # Every minute
    start_date=days_ago(0),
    catchup=False,
    tags=['ingestion', 'streaming', 'kafka', 'spark'],
    max_active_runs=1
) as dag:

    # Task 1: Fetch data from API and push to Kafka for 50 seconds
    fetch_from_kafka_streaming = BashOperator(
        task_id='fetch_from_kafka_streaming',
        bash_command='python3 /opt/airflow/collection/producers/port_producer.py'
    )

    fetch_weather_streaming = BashOperator(
        task_id='fetch_weather_streaming',
        bash_command='python3 /opt/airflow/collection/producers/weather_producer.py'
    )

    # Task 2: Consume data from Kafka and write to HDFS using Spark (Batch Mode)
    # Note: We assume the spark container is accessible or we run spark-submit from airflow container if spark-client is installed.
    # Since Airflow container has PySpark installed but might not have full Spark cluster config, 
    # we'll use the spark-submit command pointing to the Spark Master if possible, 
    # OR simpler: we use DockerOperator if available, but BashOperator with docker exec is a common hack in simple setups.
    # Given the environment, we'll try to run it via 'docker exec' if the airflow user has permissions, 
    # BUT usually Airflow runs inside a container and can't easily exec into another sibling container without socket mounting.
    # A safer bet for this "On Premise" simulation where Airflow has Spark binaries (from Dockerfile):
    # We run spark-submit in LOCAL mode or Client mode pointing to the Spark Master.
    
    # Command to submit to the Spark container (assuming Airflow can talk to Spark Master)
    # We need the kafka package.
    ingest_from_kafka_to_hdfs = BashOperator(
        task_id='ingest_from_kafka_to_hdfs',
        bash_command="""
            spark-submit \
            --master spark://spark:7077 \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 \
            /opt/airflow/ingestion/spark_jobs/spark_streaming_consumer.py --batch
        """
    )
    
    ingest_weather_to_hdfs = BashOperator(
        task_id='ingest_weather_to_hdfs',
        bash_command="""
            spark-submit \
            --master spark://spark:7077 \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 \
            /opt/airflow/ingestion/spark_jobs/spark_weather_consumer.py --batch
        """
    )

    # Serialize Spark jobs to avoid Ivy cache race conditions on startup
    fetch_from_kafka_streaming >> ingest_from_kafka_to_hdfs
    fetch_weather_streaming >> ingest_weather_to_hdfs
    
    # Ensure sequential execution of Spark jobs
    ingest_from_kafka_to_hdfs >> ingest_weather_to_hdfs
