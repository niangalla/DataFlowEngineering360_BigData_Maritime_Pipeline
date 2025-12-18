import pandas as pd
import os
from datetime import datetime, timedelta
import random
import requests
import time
import json
from dotenv import load_dotenv
from pymongo import MongoClient
from sqlalchemy import create_engine

load_dotenv()

# API
SAFECUBE_API_KEY = os.getenv('SAFECUBE_API_KEY')
URL = 'https://api.sinay.ai/schedule/api/v1/schedules/port/traffic'
DAKAR_UNLOCODE = 'SNDKR'

# MySQL
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'passer123')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'port_db')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3307))

# MongoDB
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://root:passer123@localhost:27017/')
MONGO_DB = os.getenv('MONGO_DB', 'port_db')
MONGODB_COLLECTION = os.getenv('MONGODB_PORT_COLLECTION', 'port_traffic')


def fetch_port_data(start_date=None, end_date=None):
    """Fetch port data from SafeCube API."""
    headers = {
        'API_KEY': SAFECUBE_API_KEY,
        'Accept': 'application/json'
    }
    
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    params = {
        'unlocode': DAKAR_UNLOCODE,
        'start_date': start_date,
        'end_date': end_date,
        'limit': 1000
    }
    
    print(f"Fetching data from {start_date} to {end_date}")
    
    try:
        response = requests.get(URL, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            data_section = result.get('data', {})
            arrivals = data_section.get('arrivals', [])
            departures = data_section.get('departures', [])
            
            print(f"Arrivals: {len(arrivals)}, Departures: {len(departures)}")
            
            all_traffic = []
            for arrival in arrivals:
                arrival['traffic_type'] = 'arrival'
                all_traffic.append(arrival)
            
            for departure in departures:
                departure['traffic_type'] = 'departure'
                all_traffic.append(departure)
            
            return map_to_port_traffic(all_traffic)
        else:
            print(f"Error {response.status_code}: {response.text}")
            return []
    except Exception as e:
        print(f"API Request Error: {e}")
        return []

def map_to_port_traffic(traffic_data):
    """Map raw API data to port traffic schema."""
    if not traffic_data:
        return []
    
    print(f"Processing {len(traffic_data)} records...")
    
    processed = []
    for record in traffic_data:
        if not isinstance(record, dict):
            continue
        
        traffic_type = record.get('traffic_type', 'unknown')
        date_str = record.get('date', datetime.now().isoformat())
        
        mapped = {
            "vessel_id": str(record.get('vesselImo', random.randint(100000, 999999))),
            "vessel_name": record.get('vesselName', 'Unknown'),
            "imo_number": record.get('vesselImo') or random.randint(9000000, 9999999),
            "flag": 'UNK',
            "vessel_type": 'Container Ship',
            "draft_depth": round(random.uniform(8.0, 14.0), 1),
            "arrival_time": date_str if traffic_type == 'arrival' else None,
            "departure_time": date_str if traffic_type == 'departure' else None,
            "port_zone": record.get('terminalCode', 'Nord'),
            "terminal": record.get('terminalName', 'DP World'),
            "quay_number": random.randint(1, 15),
            "pilot_required": True,
            "status": 'Scheduled Arrival' if traffic_type == 'arrival' else 'Scheduled Departure',
            "cargo_type": record.get('serviceName', 'Container'),
            "cargo_volume": random.randint(5000, 50000),
            "operation_duration": random.randint(8, 48),
            "delay_minutes": random.randint(0, 120),
            "weather_condition": random.choice(["Sunny", "Cloudy", "Partly Cloudy"]),
            "sealine": record.get('sealine'),
            "sealine_name": record.get('sealineName'),
            "service_code": record.get('serviceCode'),
            "service_name": record.get('serviceName'),
            "traffic_type": traffic_type,
            "timestamp": datetime.now().isoformat()
        }
        processed.append(mapped)
    
    print(f"Processed: {len(processed)} records")
    return processed

def save_port_data(data, output_dir=None):
    """Save port data to files and databases."""
    if not data:
        print("No data to save")
        return
    
    df = pd.DataFrame(data)
    print(f"Saving {len(df)} records...")
    
    # Files
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        csv_path = os.path.join(output_dir, "historical_port.csv")
        jsonl_path = os.path.join(output_dir, "historical_port.jsonl")
        
        df.to_csv(csv_path, index=False)
        df.to_json(jsonl_path, orient="records", lines=True, force_ascii=False)
        print(f"Files saved to {output_dir}")

    # MySQL
    try:
        engine = create_engine(
            f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}",
            echo=False
        )
        df.to_sql('port_traffic', con=engine, if_exists='replace', index=False, chunksize=200)
        print("MySQL saved")
    except Exception as e:
        print(f"MySQL error: {e}")

    # MongoDB
    try:
        client = MongoClient(MONGO_URI, authSource='admin')
        db = client[MONGO_DB]
        collection = db[MONGODB_COLLECTION]
        
        collection.drop()
        records = df.to_dict('records')
        if records:
            collection.insert_many(records)
        
        client.close()
        print("MongoDB saved")
    except Exception as e:
        print(f"MongoDB error: {e}")

if __name__ == "__main__":
    # Simple test execution
    data = fetch_port_data()
    if data:
        # Default save to relative path if run as script
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(BASE_DIR, "../../../data/source/from_apis")
        save_port_data(data, output_dir)