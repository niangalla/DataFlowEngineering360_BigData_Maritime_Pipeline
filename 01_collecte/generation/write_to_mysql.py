# write_to_mysql.py
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# Vars de connexion - à adapter si tu changes docker-compose
USER = "dbuser"
PASSWORD = "dbpass"
HOST = "localhost"
PORT = 3306
DB = "patientsdb"

engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}", echo=False)

def create_table():
    create_sql = """
    CREATE TABLE IF NOT EXISTS vitals (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      patient_id VARCHAR(50),
      timestamp DATETIME,
      temperature_c FLOAT,
      heart_rate INT,
      spo2 INT,
      lat DOUBLE,
      lon DOUBLE
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))

def insert_from_csv(csv_path):
    df = pd.read_csv(csv_path)
    # Cast timestamp ISO -> datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    # To SQL (fast) - chunked
    df.to_sql('vitals', con=engine, if_exists='append', index=False, chunksize=200)

if __name__ == "__main__":
    create_table()
    insert_from_csv("data_vitals.csv")
    print("Insertion MySQL terminée.")
