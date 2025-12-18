from datetime import datetime, timedelta
import pandas as pd
import os
import requests
from dotenv import load_dotenv
from pymongo import MongoClient
from sqlalchemy import create_engine

load_dotenv()

# MySQL
MYSQL_HOST = os.getenv('MYSQL_HOST', 'localhost')
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'passer123')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'port_db')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3307))

# MongoDB
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://root:passer123@localhost:27017/')
MONGO_DB = os.getenv('MONGO_DB', 'port_db')
MONGODB_COLLECTION = os.getenv('MONGODB_WEATHER_COLLECTION', 'historical_weather')

# Open-Meteo API (free, no key)
URL = 'https://archive-api.open-meteo.com/v1/archive'


def fetch_weather_data(start_date=None, end_date=None):
    """Fetch weather data from Open-Meteo."""
    if not start_date:
        start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
        
    params = {
        'latitude': 14.6937,
        'longitude': -17.4441,
        'start_date': start_date,
        'end_date': end_date,
        'hourly': 'temperature_2m,relative_humidity_2m,weather_code'
    }
    
    print(f"Fetching weather data from {params['start_date']} to {params['end_date']}")
    
    try:
        response = requests.get(URL, params=params, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            
            times = result['hourly']['time']
            temps = result['hourly']['temperature_2m']
            humidities = result['hourly']['relative_humidity_2m']
            conditions = result['hourly']['weather_code']
            
            data = []
            for i in range(len(times)):
                code = conditions[i]
                if code == 0:
                    condition = 'Sunny'
                elif code < 4:
                    condition = 'Cloudy'
                else:
                    condition = 'Rain'
                
                record = {
                    'timestamp': times[i],
                    'temperature': temps[i],
                    'humidity': humidities[i],
                    'condition': condition,
                }
                data.append(record)
            
            print(f"Retrieved {len(data)} weather records")
            return data
        else:
            print(f"Error {response.status_code}: {response.text}")
            return []
    except Exception as e:
        print(f"API Request Error: {e}")
        return []

def save_weather_data(data, output_dir=None):
    """Save weather data to files and databases."""
    if not data:
        print("No data to save")
        return
    
    df = pd.DataFrame(data)
    print(f"Saving {len(df)} records...")
    
    # Files
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        csv_path = os.path.join(output_dir, "historical_weather.csv")
        jsonl_path = os.path.join(output_dir, "historical_weather.jsonl")
        
        df.to_csv(csv_path, index=False)
        df.to_json(jsonl_path, orient="records", lines=True, force_ascii=False)
        print(f"Files saved to {output_dir}")
    
    # MySQL
    try:
        engine = create_engine(
            f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}",
            echo=False
        )
        df.to_sql('historical_weather', con=engine, if_exists='replace', index=False, chunksize=200)
        print("MySQL saved")
    except Exception as e:
        print(f"MySQL error: {e}")
    
    # MongoDB
    try:
        client = MongoClient(MONGO_URI, authSource='admin')
        db = client[MONGO_DB]
        collection = db[MONGODB_COLLECTION]
        
        collection.drop()
        collection.insert_many(df.to_dict('records'))
        
        client.close()
        print("MongoDB saved")
    except Exception as e:
        print(f"MongoDB error: {e}")

if __name__ == "__main__":
    # Simple test execution
    data = fetch_weather_data()
    if data:
        # Default save to relative path if run as script
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(BASE_DIR, "../../../data/source/from_apis")
        save_weather_data(data, output_dir)