import json
from datetime import datetime
from kafka import KafkaProducer  # pip install kafka-python
import requests

# === CONFIG ===
OPENWEATHER_API_KEY = 'TA_CLÉ_OPENWEATHER'
WEATHER_TOPIC = 'weather_stream'

# Producer Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# === FETCH OPENWEATHER ===
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
        return [weather]
    else:
        print("Erreur OpenWeather:", response.status_code)
        return [{'temperature': 30.0, 'humidity': 70, 'condition': 'Sunny', 'timestamp': datetime.now().isoformat()}]

# === PRODUCE ===
def produce_weather(data):
    for record in data:
        producer.send(WEATHER_TOPIC, record)
    producer.flush()
    print(f"Produit {len(data)} météo vers '{WEATHER_TOPIC}'")

# Exécute un cycle unique
weather = fetch_openweather()
produce_weather(weather)