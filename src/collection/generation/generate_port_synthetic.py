import pandas as pd
from faker import Faker
import random
import os
from datetime import timedelta

fake = Faker()

# Listes réalistes (comme ton script)
vessel_names = [
    "MV NIANING", "MV GOREE", "MV ATLANTIC", "MV SENEGAL STAR", "SS NDAR",
    "MV KAOLACK", "MV YOFF", "MV NGOR", "MV THIAROYE", "MV DAKAR SPIRIT"
]
vessel_types = ["Container", "Tanker", "Bulk Carrier", "General Cargo", "Ro-Ro", "Passenger"]
flags = ["SEN", "MLI", "CIV", "FRA", "ESP", "GHA", "NGA"]
cargo_types = ["Containers", "Bulk", "Oil", "Vehicles", "Fishery"]
terminals = ["DP World", "Socopao", "Bolloré Africa Logistics", "Petrosen Terminal"]
zones = ["Nord", "Sud", "Ndayane"]
weather_conditions = ["Sunny", "Cloudy", "Rain", "Windy", "Storm"]

import mysql.connector
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# Configuration DB
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'passer123')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'port_db')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3307))

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://root:passer123@localhost:27017/')
MONGO_DB = os.getenv('MONGO_DB', 'port_db')
MONGODB_COLLECTION = os.getenv('MONGODB_PORT_COLLECTION', 'port_traffic')

# Génération 2000 escales pour couvrir la période
data = []
number_of_samples = 2000
start_date = pd.Timestamp("2025-11-01")
end_date = pd.Timestamp("2026-02-01")
date_range_days = (end_date - start_date).days

for _ in range(number_of_samples):
    # Random date within range
    random_days = random.randint(0, date_range_days)
    arrival = start_date + timedelta(days=random_days, hours=random.randint(0, 23), minutes=random.randint(0, 59))
    
    operation_hours = random.randint(8, 72)
    departure = arrival + timedelta(hours=operation_hours)

    record = {
        "vessel_id": fake.uuid4(),
        "vessel_name": random.choice(vessel_names),
        "imo_number": fake.random_int(min=9000000, max=9999999),
        "flag": random.choice(flags),
        "vessel_type": random.choice(vessel_types),
        "draft_depth": round(random.uniform(6.0, 15.0), 1),
        "arrival_time": arrival.isoformat(),
        "departure_time": departure.isoformat(),
        "port_zone": random.choice(zones),
        "terminal": random.choice(terminals),
        "quay_number": random.randint(1, 15),
        "pilot_required": random.choice([True, False]),
        "status": random.choice(["Arrived", "Unloading", "Departed", "Anchored"]),
        "cargo_type": random.choice(cargo_types),
        "cargo_volume": random.randint(500, 200000),
        "operation_duration": operation_hours,
        "delay_minutes": random.randint(0, 240),
        "weather_condition": random.choice(weather_conditions)
    }
    data.append(record)

df = pd.DataFrame(data)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Adjusted relative path to match container structure:
# /opt/airflow/collection/generation -> ../../data -> /opt/airflow/data
OUTPUT_DIR = os.path.join(BASE_DIR, "../../data/source/synthetic")
os.makedirs(OUTPUT_DIR, exist_ok=True)

jsonl_path = os.path.join(OUTPUT_DIR, "synthetic_port_data.jsonl")

# Chaque ligne = un objet JSON (orient='records', lines=True)
df.to_json(jsonl_path, orient='records', lines=True, force_ascii=False)
df.to_csv(os.path.join(OUTPUT_DIR, "synthetic_port_data.csv"), index=False)

print(f"Generated {len(data)} records locally.")

# Stockage MySQL
try:
    print(f"Connecting to MySQL {MYSQL_HOST}:{MYSQL_PORT}...")
    conn = mysql.connector.connect(
        host=MYSQL_HOST, 
        user=MYSQL_USER, 
        password=MYSQL_PASSWORD, 
        database=MYSQL_DATABASE,
        port=MYSQL_PORT
    )
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS port_traffic (
        vessel_id VARCHAR(36) PRIMARY KEY, vessel_name VARCHAR(255), imo_number INT, flag VARCHAR(10),
        vessel_type VARCHAR(50), draft_depth FLOAT, arrival_time DATETIME, departure_time DATETIME,
        port_zone VARCHAR(50), terminal VARCHAR(100), quay_number INT, pilot_required BOOLEAN,
        status VARCHAR(50), cargo_type VARCHAR(50), cargo_volume INT, operation_duration INT,
        delay_minutes INT, weather_condition VARCHAR(50)
    )''')
    
    for row in data:
        cursor.execute('''INSERT IGNORE INTO port_traffic VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                       (row['vessel_id'], row['vessel_name'], row['imo_number'], row['flag'], row['vessel_type'], row['draft_depth'],
                        row['arrival_time'], row['departure_time'], row['port_zone'], row['terminal'], row['quay_number'],
                        1 if row['pilot_required'] else 0, row['status'], row['cargo_type'], row['cargo_volume'],
                        row['operation_duration'], row['delay_minutes'], row['weather_condition']))
    conn.commit()
    conn.close()
    print("✅ Data saved to MySQL successfully!")
except Exception as e:
    print(f"❌ MySQL Error: {e}")

# Stockage MongoDB
try:
    print(f"Connecting to MongoDB {MONGO_URI}...")
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB]
    collection = db[MONGODB_COLLECTION]
    # Optional: Clear existing data to avoid duplicates on re-run
    # collection.delete_many({}) 
    collection.insert_many(data)
    mongo_client.close()
    print("✅ Data saved to MongoDB successfully!")
except Exception as e:
    print(f"❌ MongoDB Error: {e}")

print(f"{number_of_samples} synthetic records generated and stored!")