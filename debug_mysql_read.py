import pandas as pd
from sqlalchemy import create_engine
import os

MYSQL_HOST = os.getenv('MYSQL_HOST', 'mysql')
MYSQL_USER = os.getenv('MYSQL_USER', 'dbuser')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'passer123')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'port_db')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))

connection_string = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
print(f"Connecting to {connection_string}...")

try:
    engine = create_engine(connection_string)
    query = "SELECT * FROM historical_weather"
    print(f"Executing query: {query}")
    
    df = pd.read_sql(query, engine)
    print(f"Read {len(df)} records.")
    print(df.head())
    
    # Test chunked reading
    print("Testing chunked reading...")
    chunks = pd.read_sql(query, engine, chunksize=1000)
    for i, chunk in enumerate(chunks):
        print(f"Chunk {i}: {len(chunk)} records")
        
except Exception as e:
    print(f"Error: {e}")
