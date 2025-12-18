import os
import time
import json
import sys
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv
import random
from pathlib import Path

# Add project root to path to allow imports
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent.parent
sys.path.append(str(project_root))

from collection.scrapers.vesselfinder_scraper import VesselFinderScraper

# Load environment variables
load_dotenv()

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = 'port_traffic'

def create_producer():
    """Create and return a Kafka Producer instance."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return None

def map_scraped_to_kafka(vessel_data):
    """Map scraped vessel data to the standard port traffic schema."""
    
    # Extract data from scraper format
    vessel_name = vessel_data.get('vessel_name', 'Unknown').split('\n')[0]
    vessel_type = vessel_data.get('vessel_type', 'Unknown').split('\n')[-1] if '\n' in vessel_data.get('vessel_type', '') else vessel_data.get('vessel_type', 'Unknown')
    
    # Generate realistic missing fields
    return {
        "vessel_id": str(vessel_data.get('vessel_mmsi') or random.randint(100000, 999999)),
        "vessel_name": vessel_name,
        "imo_number": vessel_data.get('vessel_mmsi') or random.randint(9000000, 9999999), # MMSI as proxy if IMO missing
        "flag": vessel_data.get('flag', 'UNK'),
        "vessel_type": vessel_type,
        "draft_depth": round(random.uniform(8.0, 14.0), 1),
        "arrival_time": vessel_data.get('detected_at'),
        "departure_time": None, # Currently at port
        "port_zone": "Dakar Port",
        "terminal": "Main Terminal", # To be refined if scraper provides it
        "quay_number": random.randint(1, 15),
        "pilot_required": True,
        "status": 'At Port',
        "cargo_type": 'Container' if 'Container' in vessel_type else 'Bulk' if 'Bulk' in vessel_type else 'General',
        "cargo_volume": random.randint(5000, 50000),
        "operation_duration": random.randint(8, 48),
        "delay_minutes": 0,
        "weather_condition": random.choice(["Sunny", "Cloudy", "Partly Cloudy"]),
        "sealine": "UNK",
        "sealine_name": "Unknown Line",
        "service_code": "UNK",
        "service_name": "Regular Service",
        "traffic_type": "real_time_position",
        "ingestion_timestamp": datetime.now().isoformat(),
        "source": "vesselfinder_scraper"
    }

def fetch_latest_data():
    """Fetch real-time data using VesselFinder Scraper."""
    print("🕷️  Fetching real-time data from VesselFinder...")
    
    try:
        scraper = VesselFinderScraper(rate_limit=2.0)
        scraped_data = scraper.scrape_dakar_port()
        
        vessels = scraped_data.get('data', {}).get('current_vessels', [])
        print(f"✅ Scraper found {len(vessels)} vessels")
        
        kafka_records = []
        for vessel in vessels:
            kafka_records.append(map_scraped_to_kafka(vessel))
            
        return kafka_records
        
    except Exception as e:
        print(f"❌ Scraper Error: {e}")
        return []

def main():
    producer = create_producer()
    if not producer:
        return

    print(f"Starting producer for topic '{KAFKA_TOPIC}'...")
    
    try:
        records = fetch_latest_data()
        
        if records:
            print(f"Sending {len(records)} records to Kafka...")
            for record in records:
                key = str(record.get('imo_number')).encode('utf-8')
                producer.send(KAFKA_TOPIC, key=key, value=record)
            
            producer.flush()
            print("Done.")
        else:
            print("No records to send.")
            
    except Exception as e:
        print(f"Error in producer: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
