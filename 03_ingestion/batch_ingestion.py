# ingest/ingest_batch.py
import json, os
from sqlalchemy import create_engine, text
from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
from tqdm import tqdm

load_dotenv()

MYSQL_URL = f"mysql+pymysql://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}@{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DB')}"
engine = create_engine(MYSQL_URL)

MONGO_URI = os.getenv("MONGO_URI")
mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client[os.getenv("MONGO_DB")]

def upsert_sportifs(sportifs_path="../02_source_donnees/sportifs.json"):
    if not os.path.exists(sportifs_path):
        print("No sportifs file found.")
        return
    with open(sportifs_path) as f:
        sportifs = json.load(f)
    df = pd.DataFrame(sportifs)
    # convert missing team -> None
    df['team'] = df['team'].where(pd.notnull(df['team']), None)
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
            INSERT INTO sportifs(sportif_id, name, age, sex, team, sport, weight_kg, height_cm, level)
            VALUES (:sportif_id, :name, :age, :sex, :team, :sport, :weight_kg, :height_cm, :level)
            ON DUPLICATE KEY UPDATE name=VALUES(name), age=VALUES(age), team=VALUES(team), sport=VALUES(sport), weight_kg=VALUES(weight_kg), height_cm=VALUES(height_cm), level=VALUES(level)
            """), row.to_dict())
    # also upsert into Mongo sportifs collection
    # mongo_db.sportifs.delete_many({})  # optional: reset
    # mongo_db.sportifs.insert_many(sportifs)
    print(f"Upserted {len(df)} sportifs to MySQL")

def ingest_seances(jsonl_path="../02_source_donnees/seances.jsonl"):
    if not os.path.exists(jsonl_path):
        print("No seances file found.")
        return
    records = []
    with open(jsonl_path) as f:
        for line in f:
            records.append(json.loads(line))
    print(f"Loaded {len(records)} seances")
    # Insert to Mongo (raw)
    mongo_db.seances.insert_many(records)
    print(f"Inserted {len(records)} documents into MongoDB (seances)")

    # Insert to MySQL (structured)
    # df = pd.DataFrame(records)
    # # convert timestamp to datetime
    # df['timestamp'] = pd.to_datetime(df['timestamp'])
    # # Ensure columns order consistent with init.sql
    # cols = ['sportif_id','seance_id','timestamp','seance_type','duration_sec','distance_km',
    #         'avg_speed_kmh','max_speed_kmh','avg_heart_rate','max_heart_rate','calories',
    #         'lat','lon','fatigue_score','injury_flag']
    # df = df[cols]
    # df.to_sql('seances', con=engine, if_exists='append', index=False, chunksize=500)
    # print(f"Inserted {len(df)} rows into MySQL (seances)")

if __name__ == "__main__":
    upsert_sportifs()
    ingest_seances()
    print("Batch ingestion completed.")
