import json
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer  # pip install kafka-python
from kpler.sdk import KplerClient  # pip install kpler-sdk

# === CONFIG ===
KPLER_API_KEY = 'TA_CLÉ_KPLER'
KPLER_TOPIC = 'port_stream'

# Client Kpler
kpler_client = KplerClient(api_key=KPLER_API_KEY)

# Producer Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# === FETCH KPLER (pour updates récentes) ===
def fetch_kpler():
    start_date = (datetime.now() - timedelta(minutes=5)).isoformat()  # Dernières 5 min pour near real-time
    end_date = datetime.now().isoformat()
    port_calls = kpler_client.port_calls(
        port='Dakar',
        from_date=start_date,
        to_date=end_date,
        commodity='all',
        limit=100
    )
    if not port_calls:
        print("Aucune data Kpler.")
        return []
    return port_calls

# === MAPPING AUX CHAMPS ===
def map_kpler_to_fields(port_calls):
    processed_data = []
    for call in port_calls:
        record = {
            "vessel_id": call.get('mmsi', str(random.randint(100000, 999999))),
            "vessel_name": call.get('ship_name', 'Unknown Vessel'),
            "imo_number": call.get('imo', random.randint(9000000, 9999999)),
            "flag": call.get('flag', 'UNK'),
            "vessel_type": call.get('vessel_type', 'Unknown'),
            "draft_depth": call.get('draught', round(random.uniform(6.0, 15.0), 1)),
            "arrival_time": call.get('eta', datetime.now().isoformat()),
            "departure_time": call.get('etd', (datetime.now() + timedelta(hours=random.randint(8, 72))).isoformat()),
            "port_zone": call.get('zone', 'Nord'),
            "terminal": call.get('terminal', 'DP World'),
            "quay_number": random.randint(1, 15),
            "pilot_required": random.choice([True, False]),
            "status": call.get('status', 'Arrived'),
            "cargo_type": call.get('commodity', 'General Cargo'),
            "cargo_volume": call.get('quantity', random.randint(500, 200000)),
            "operation_duration": random.randint(8, 72),
            "delay_minutes": random.randint(0, 240),
            "weather_condition": random.choice(["Sunny", "Cloudy", "Rain"])
        }
        processed_data.append(record)
    return processed_data

# === PRODUCE ===
def produce_kpler(data):
    for record in data:
        producer.send(KPLER_TOPIC, record)
    producer.flush()
    print(f"Produit {len(data)} escales Kpler vers '{KPLER_TOPIC}'")

# Exécute un cycle unique
port_calls = fetch_kpler()
processed = map_kpler_to_fields(port_calls)
produce_kpler(processed)