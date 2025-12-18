from kafka import KafkaProducer
import json
import requests
from datetime import datetime
import os

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
TOPIC = 'weather_realtime'
URL = 'https://archive-api.open-meteo.com/v1/archive' # Using archive for consistency or forecast for current?
# For real-time, we should use the forecast API which gives current weather
CURRENT_URL = 'https://api.open-meteo.com/v1/forecast'

def fetch_current_weather():
    params = {
        'latitude': 14.6937,
        'longitude': -17.4441,
        'current': 'temperature_2m,relative_humidity_2m,weather_code',
        'timezone': 'auto'
    }
    
    try:
        response = requests.get(CURRENT_URL, params=params, timeout=10)
        if response.status_code == 200:
            result = response.json()
            current = result.get('current', {})
            
            code = current.get('weather_code', 0)
            if code == 0:
                condition = 'Sunny'
            elif code < 4:
                condition = 'Cloudy'
            else:
                condition = 'Rain'
                
            record = {
                'temperature': current.get('temperature_2m'),
                'humidity': current.get('relative_humidity_2m'),
                'condition': condition,
                'timestamp': datetime.now().isoformat(),
                'source': 'open-meteo-realtime'
            }
            return record
        else:
            print(f"API Error: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Request Error: {e}")
        return None

def main():
    print(f"Starting Weather Producer to {KAFKA_BOOTSTRAP_SERVERS}...")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        record = fetch_current_weather()
        
        if record:
            print(f"Sending record: {record}")
            producer.send(TOPIC, record)
            producer.flush()
            print("Message sent successfully.")
        else:
            print("No data to send.")
            
        producer.close()
        
    except Exception as e:
        print(f"Kafka Error: {e}")

if __name__ == "__main__":
    main()