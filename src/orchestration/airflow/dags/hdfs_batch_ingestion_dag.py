from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
import json
import os
from sqlalchemy import create_engine
from pymongo import MongoClient
from hdfs import InsecureClient
import glob

# Configuration
MYSQL_HOST = os.getenv('MYSQL_HOST', 'mysql')
MYSQL_USER = os.getenv('MYSQL_USER', 'dbuser')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'passer123')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'port_db')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://root:passer123@mongo:27017/')
MONGO_DB = os.getenv('MONGO_DB', 'port_db')
MONGO_COLLECTION = os.getenv('MONGODB_PORT_COLLECTION', 'port_traffic')
MONGODB_WEATHER_COLLECTION = os.getenv('MONGODB_WEATHER_COLLECTION', 'historical_weather')
MYSQL_WEATHER_TABLE = 'historical_weather'

HDFS_URL = os.getenv('HDFS_URL', 'http://namenode:9870')
HDFS_USER = os.getenv('HDFS_USER', 'root')
HDFS_BASE_PATH = '/datalake/raw_zone/historical'

SOURCE_DIRS = [
    '/opt/airflow/data/source/from_apis',
    '/opt/airflow/data/source/scraped'
]

CHUNK_SIZE = 1000

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_hdfs_client():
    return InsecureClient(HDFS_URL, user=HDFS_USER)

def ingest_port_mysql_chunked(**context):
    """Ingest from MySQL to HDFS in chunks."""
    try:
        connection_string = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
        engine = create_engine(connection_string)
        
        # Use chunksize to return an iterator
        query = "SELECT * FROM port_traffic"
        chunks = pd.read_sql(query, engine, chunksize=CHUNK_SIZE)
        
        client = get_hdfs_client()
        execution_date = context['ds']
        
        total_records = 0
        for i, df in enumerate(chunks):
            if df.empty:
                continue
                
            path = f"{HDFS_BASE_PATH}/mysql/traffic_{execution_date}_part{i}.jsonl"
            
            records = df.to_dict(orient='records')
            content = "\n".join([json.dumps(r, default=str) for r in records])
            
            with client.write(path, encoding='utf-8', overwrite=True) as writer:
                writer.write(content)
            
            total_records += len(records)
            print(f"MySQL: Wrote chunk {i} ({len(records)} records) to {path}")
            
        print(f"MySQL: Total ingested {total_records} records")
        
    except Exception as e:
        print(f"MySQL Error: {e}")
        raise

def ingest_port_mongodb_chunked(**context):
    """Ingest from MongoDB to HDFS in chunks."""
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # Count total documents
        total_docs = collection.count_documents({})
        if total_docs == 0:
            print("No data in MongoDB")
            return

        hdfs_client = get_hdfs_client()
        execution_date = context['ds']
        
        # Process in chunks using skip/limit or batch_size
        # Using skip/limit for simplicity in logic, though cursor iteration is better for consistency
        # Here we'll iterate the cursor and batch manually
        
        cursor = collection.find({})
        batch = []
        part_num = 0
        
        def mongo_converter(o):
            if isinstance(o, datetime):
                return o.isoformat()
            if hasattr(o, '__str__'):
                return str(o)
            return o

        for doc in cursor:
            batch.append(doc)
            if len(batch) >= CHUNK_SIZE:
                path = f"{HDFS_BASE_PATH}/mongodb/traffic_{execution_date}_part{part_num}.jsonl"
                
                # Ensure directory exists
                client.makedirs(os.path.dirname(path))

                content = "\n".join([json.dumps(r, default=mongo_converter) for r in batch])
                
                with hdfs_client.write(path, encoding='utf-8', overwrite=True) as writer:
                    writer.write(content)
                
                print(f"MongoDB: Wrote chunk {part_num} to {path}")
                batch = []
                part_num += 1
        
        # Write remaining
        if batch:
            path = f"{HDFS_BASE_PATH}/mongodb/traffic_{execution_date}_part{part_num}.jsonl"
            content = "\n".join([json.dumps(r, default=mongo_converter) for r in batch])
            with hdfs_client.write(path, encoding='utf-8', overwrite=True) as writer:
                writer.write(content)
            print(f"MongoDB: Wrote final chunk {part_num} to {path}")
            
    except Exception as e:
        print(f"MongoDB Error: {e}")
        raise

def ingest_weather_mysql_chunked(**context):
    """Ingest weather from MySQL to HDFS in chunks."""
    try:
        connection_string = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
        engine = create_engine(connection_string)
        
        query = f"SELECT * FROM {MYSQL_WEATHER_TABLE}"
        chunks = pd.read_sql(query, engine, chunksize=CHUNK_SIZE)
        
        client = get_hdfs_client()
        execution_date = context['ds']
        
        total_records = 0
        for i, df in enumerate(chunks):
            if df.empty:
                continue
                
            path = f"{HDFS_BASE_PATH}/weather/mysql/weather_{execution_date}_part{i}.jsonl"
            
            # Ensure directory exists
            client.makedirs(os.path.dirname(path))
            
            records = df.to_dict(orient='records')
            content = "\n".join([json.dumps(r, default=str) for r in records])
            
            with client.write(path, encoding='utf-8', overwrite=True) as writer:
                writer.write(content)
            
            total_records += len(records)
            print(f"MySQL Weather: Wrote chunk {i} ({len(records)} records) to {path}")
            
        print(f"MySQL Weather: Total ingested {total_records} records")
        
    except Exception as e:
        print(f"MySQL Weather Error: {e}")
        raise

def ingest_weather_mongodb_chunked(**context):
    """Ingest weather from MongoDB to HDFS in chunks."""
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGODB_WEATHER_COLLECTION]
        
        total_docs = collection.count_documents({})
        if total_docs == 0:
            print("No weather data in MongoDB")
            return

        hdfs_client = get_hdfs_client()
        execution_date = context['ds']
        
        cursor = collection.find({})
        batch = []
        part_num = 0
        
        def mongo_converter(o):
            if isinstance(o, datetime):
                return o.isoformat()
            if hasattr(o, '__str__'):
                return str(o)
            return o

        for doc in cursor:
            batch.append(doc)
            if len(batch) >= CHUNK_SIZE:
                path = f"{HDFS_BASE_PATH}/weather/mongodb/weather_{execution_date}_part{part_num}.jsonl"
                
                # Ensure directory exists
                hdfs_client.makedirs(os.path.dirname(path))
                
                content = "\n".join([json.dumps(r, default=mongo_converter) for r in batch])
                
                with hdfs_client.write(path, encoding='utf-8', overwrite=True) as writer:
                    writer.write(content)
                
                print(f"MongoDB Weather: Wrote chunk {part_num} to {path}")
                batch = []
                part_num += 1
        
        if batch:
            path = f"{HDFS_BASE_PATH}/weather/mongodb/weather_{execution_date}_part{part_num}.jsonl"
            content = "\n".join([json.dumps(r, default=mongo_converter) for r in batch])
            with hdfs_client.write(path, encoding='utf-8', overwrite=True) as writer:
                writer.write(content)
            print(f"MongoDB Weather: Wrote final chunk {part_num} to {path}")
            
    except Exception as e:
        print(f"MongoDB Weather Error: {e}")
        raise

def ingest_files_chunked(**context):
    """Ingest local CSV/JSONL files to HDFS in chunks, standardizing to JSONL."""
    try:
        hdfs_client = get_hdfs_client()
        execution_date = context['ds']
        target_dir = f"{HDFS_BASE_PATH}/files/{execution_date}"
        hdfs_client.makedirs(target_dir)
        
        for source_dir in SOURCE_DIRS:
            if not os.path.exists(source_dir):
                print(f"Directory not found: {source_dir}, skipping...")
                continue
                
            files = glob.glob(f"{source_dir}/*")
            print(f"Found {len(files)} files in {source_dir}")
            
            for file_path in files:
                filename = os.path.basename(file_path)
                base_name, ext = os.path.splitext(filename)
                ext = ext.lower()
                
                print(f"Processing {filename}...")
                
                chunks = []
                if ext == '.csv':
                    chunks = pd.read_csv(file_path, chunksize=CHUNK_SIZE)
                elif ext in ['.json', '.jsonl']:
                    # For JSON, we assume lines=True for JSONL or standard JSON list
                    try:
                        chunks = pd.read_json(file_path, lines=True, chunksize=CHUNK_SIZE)
                    except ValueError:
                        # Fallback for standard JSON list
                        chunks = pd.read_json(file_path, chunksize=CHUNK_SIZE)
                else:
                    print(f"Skipping unsupported file type: {filename}")
                    continue
                    
                total_records = 0
                for i, df in enumerate(chunks):
                    if df.empty:
                        continue
                    
                    # Standardize output filename: originalname_partX.jsonl
                    hdfs_path = f"{target_dir}/{base_name}_part{i}.jsonl"
                    
                    records = df.to_dict(orient='records')
                    content = "\n".join([json.dumps(r, default=str) for r in records])
                    
                    with hdfs_client.write(hdfs_path, encoding='utf-8', overwrite=True) as writer:
                        writer.write(content)
                    
                    total_records += len(records)
                print(f"Wrote chunk {i} ({len(records)} records) to {hdfs_path}")
                
                print(f"Finished {filename}: {total_records} records ingested")
            
    except Exception as e:
        print(f"File Ingestion Error: {e}")
        raise

with DAG(
    'hdfs_batch_ingestion',
    default_args=default_args,
    description='Ingest data from MySQL/MongoDB/Files to HDFS',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,  # Prevent concurrent runs causing HDFS lease issues
    tags=['ingestion', 'batch', 'hdfs'],
) as dag:

    t1 = PythonOperator(
        task_id='ingest_port_mysql',
        python_callable=ingest_port_mysql_chunked,
    )

    t2 = PythonOperator(
        task_id='ingest_port_mongodb',
        python_callable=ingest_port_mongodb_chunked,
    )

    t3 = PythonOperator(
        task_id='ingest_files',
        python_callable=ingest_files_chunked,
    )

    t4 = PythonOperator(
        task_id='ingest_weather_mysql',
        python_callable=ingest_weather_mysql_chunked,
    )

    t5 = PythonOperator(
        task_id='ingest_weather_mongodb',
        python_callable=ingest_weather_mongodb_chunked,
    )

    trigger_transform = TriggerDagRunOperator(
        task_id='trigger_transformation',
        trigger_dag_id='transformation_full',
        wait_for_completion=False
    )

    trigger_streaming = TriggerDagRunOperator(
        task_id='trigger_streaming',
        trigger_dag_id='streaming_ingestion',
        wait_for_completion=False
    )

    # All ingestion tasks must complete before triggering downstream DAGs
    ingestion_tasks = [t1, t2, t3, t4, t5]
    
    # Trigger Streaming first (to start consumers), then Transformation (to process batch data)
    ingestion_tasks >> trigger_streaming >> trigger_transform
