# ingest_batch.py
import json
import pandas as pd
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from dotenv import load_dotenv
import os
from tqdm import tqdm

# Charger les variables d'environnement
load_dotenv()

# -------------------------------
# Connexions aux bases
# -------------------------------
mysql_engine = create_engine(
    f"mysql+pymysql://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}"
    f"@{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DB')}"
)

mongo_client = MongoClient(os.getenv("MONGO_URI"))
mongo_db = mongo_client[os.getenv("MONGO_DB")]
mongo_col = mongo_db["vitals"]

# -------------------------------
# Création table MySQL (si absente)
# -------------------------------
def create_mysql_table():
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
    with mysql_engine.begin() as conn:
        conn.execute(text(create_sql))
    print("Table vitals prête dans MySQL")

# -------------------------------
# Lecture du fichier JSONL
# -------------------------------
def read_jsonl(path):
    data = []
    with open(path, "r") as f:
        for line in f:
            try:
                data.append(json.loads(line.strip()))
            except json.JSONDecodeError:
                continue
    return data

# -------------------------------
# Insertion MongoDB
# -------------------------------
def insert_mongo(records):
    if not records:
        return
    mongo_col.insert_many(records)
    print(f"{len(records)} documents insérés dans MongoDB")

# -------------------------------
# Insertion MySQL
# -------------------------------
def insert_mysql(records):
    if not records:
        return
    df = pd.DataFrame(records)
    # Convertir timestamp → datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df.to_sql("vitals", con=mysql_engine, if_exists="append", index=False, chunksize=200)
    print(f"{len(records)} lignes insérées dans MySQL")

# -------------------------------
# Pipeline principal
# -------------------------------
if __name__ == "__main__":
    create_mysql_table()

    file_path = "data_vitals.jsonl"
    if not os.path.exists(file_path):
        print("Fichier data_vitals.jsonl introuvable. Lance d'abord ton générateur !")
        exit()

    records = read_jsonl(file_path)
    print(f"{len(records)} enregistrements lus depuis {file_path}")

    # Insérer dans MongoDB
    insert_mongo(records)

    # Insérer dans MySQL
    insert_mysql(records)

    print("\nIngestion batch terminée avec succès.")
