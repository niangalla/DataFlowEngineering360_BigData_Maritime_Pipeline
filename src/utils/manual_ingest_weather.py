
import pandas as pd
import json
import os
from sqlalchemy import create_engine
from hdfs import InsecureClient
from datetime import datetime

# Configuration
MYSQL_HOST = 'mysql'
MYSQL_USER = 'dbuser'
MYSQL_PASSWORD = 'passer123'
MYSQL_DATABASE = 'port_db'
MYSQL_PORT = 3306
MYSQL_WEATHER_TABLE = 'historical_weather'

HDFS_URL = 'http://namenode:9870'
HDFS_USER = 'root'
HDFS_BASE_PATH = '/datalake/raw_zone/historical/weather/mysql'

def ingest_weather():
    print("Connecting to MySQL...")
    connection_string = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    engine = create_engine(connection_string)
    
    query = f"SELECT * FROM {MYSQL_WEATHER_TABLE}"
    chunks = pd.read_sql(query, engine, chunksize=1000)
    
    print("Connecting to HDFS...")
    client = InsecureClient(HDFS_URL, user=HDFS_USER)
    execution_date = datetime.now().strftime('%Y-%m-%d')
    
    total_records = 0
    for i, df in enumerate(chunks):
        if df.empty:
            continue
            
        path = f"{HDFS_BASE_PATH}/weather_{execution_date}_part{i}.jsonl"
        
        records = df.to_dict(orient='records')
        content = "\n".join([json.dumps(r, default=str) for r in records])
        
        with client.write(path, encoding='utf-8', overwrite=True) as writer:
            writer.write(content)
        
        total_records += len(records)
        print(f"Wrote chunk {i} ({len(records)} records) to {path}")
        
    print(f"Total ingested {total_records} records")

if __name__ == "__main__":
    try:
        ingest_weather()
    except Exception as e:
        print(f"Error: {e}")
