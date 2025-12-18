import requests
import json
from datetime import datetime, timedelta

# Open-Meteo Archive API (Free, No Key)
URL = 'https://archive-api.open-meteo.com/v1/archive'

# Test Period: Last Month (Nov 2025)
start_date = "2025-11-01"
end_date = "2025-11-05"

params = {
    'latitude': 14.6937, # Dakar
    'longitude': -17.4441,
    'start_date': start_date,
    'end_date': end_date,
    'hourly': 'temperature_2m,relative_humidity_2m,weather_code'
}

print(f"Testing Weather API: {URL}")
print(f"Period: {start_date} to {end_date}")

try:
    response = requests.get(URL, params=params, timeout=10)
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        hourly = data.get('hourly', {})
        times = hourly.get('time', [])
        temps = hourly.get('temperature_2m', [])
        
        print(f"Records found: {len(times)}")
        if times:
            print(f"First Record: {times[0]} -> {temps[0]}°C")
            print(f"Last Record:  {times[-1]} -> {temps[-1]}°C")
            print("✅ SUCCESS: Historical Weather Data Retrieved!")
        else:
            print("⚠️  WARNING: No data points returned.")
    else:
        print(f"❌ ERROR: {response.text}")

except Exception as e:
    print(f"💥 EXCEPTION: {e}")
