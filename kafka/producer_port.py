import argparse
import json
import time
import random
import requests
from datetime import datetime, timedelta
from kafka import KafkaProducer  # pip install kafka-python
from kpler.sdk import KplerClient  # pip install kpler-sdk

# === CONFIGURATION ===
# Remplace par tes clés réelles (inscris-toi sur developers.kpler.com et openweathermap.org)
KPLER_API_KEY = 'TA_CLÉ_KPLER'
OPENWEATHER_API_KEY = 'TA_CLÉ_OPENWEATHER'

# Client Kpler
kpler_client = KplerClient(api_key=KPLER_API_KEY)

# Producer Kafka (assume broker local)
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Topics Kafka
PORT_TOPIC = 'port_stream'  # Pour données Kpler (escales/navires)
WEATHER_TOPIC = 'weather_stream'  # Pour données météo OpenWeather

# Parser pour mode (historical ou real_time)
parser = argparse.ArgumentParser(description='Producer for Kpler and OpenWeather data to Kafka.')
parser.add_argument('--mode', type=str, default='historical', choices=['historical', 'real_time'], help='Mode: historical or real_time')
args = parser.parse_args()

# === FETCH KPLER (adapté pour Dakar) ===
def fetch_kpler(start_date, end_date):
    # Fetch Port Calls (historical ou near real-time via params)
    port_calls = kpler_client.port_calls(
        port='Dakar',  # Ou ID spécifique
        from_date=start_date,
        to_date=end_date,
        commodity='all',
        limit=100  # Ajuste pour tes limites
    )
    if not port_calls:
        print("Aucune data Kpler - vérifie clé ou limites.")
        return []
    return port_calls

# === FETCH OPENWEATHER (météo pour Dakar) ===
def fetch_openweather():
    url = f'https://api.openweathermap.org/data/2.5/weather?q=Dakar,SN&appid={OPENWEATHER_API_KEY}&units=metric'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        weather = {
            'temperature': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'condition': data['weather'][0]['description'],
            'timestamp': datetime.now().isoformat()
        }
        return weather
    else:
        print("Erreur OpenWeather:", response.status_code)
        return {'temperature': random.uniform(25, 35), 'humidity': random.randint(50, 90), 'condition': 'Sunny', 'timestamp': datetime.now().isoformat()}  # Simulé si erreur

# === MAPPING KPLER AUX CHAMPS GÉNÉRÉS ===
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
            "weather_condition": random.choice(["Sunny", "Cloudy", "Rain"])  # À remplacer par fetch_openweather si intégré ici
        }
        processed_data.append(record)
    return processed_data

# === PRODUCE TO KAFKA ===
def produce_to_kafka(data, topic):
    for record in data:
        producer.send(topic, record)
    producer.flush()
    print(f"Produit {len(data)} enregistrements vers topic '{topic}'")

# === MODE HISTORICAL (batch unique) ===
if args.mode == 'historical':
    start_date = (datetime.now() - timedelta(days=30)).isoformat()  # Dernier mois
    end_date = datetime.now().isoformat()
    port_calls = fetch_kpler(start_date, end_date)
    processed_port = map_kpler_to_fields(port_calls)
    weather = [fetch_openweather()]  # Fetch unique pour batch

    produce_to_kafka(processed_port, PORT_TOPIC)
    produce_to_kafka(weather, WEATHER_TOPIC)

# === MODE REAL_TIME (polling continu) ===
elif args.mode == 'real_time':
    while True:  # Boucle infinie - arrête avec Ctrl+C
        start_date = (datetime.now() - timedelta(minutes=5)).isoformat()  # Dernières 5 min pour updates
        end_date = datetime.now().isoformat()
        port_calls = fetch_kpler(start_date, end_date)
        processed_port = map_kpler_to_fields(port_calls)
        weather = [fetch_openweather()]  # Fetch météo live

        if processed_port or weather:
            produce_to_kafka(processed_port, PORT_TOPIC)
            produce_to_kafka(weather, WEATHER_TOPIC)

        time.sleep(300)  # Polling toutes 5 min - ajuste pour limites API

print("Producer terminé !")