import os
import requests
import json
from datetime import datetime
# Manually load .env
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '.env')
if os.path.exists(env_path):
    with open(env_path, 'r') as f:
        for line in f:
            if line.strip() and not line.startswith('#'):
                key, value = line.strip().split('=', 1)
                os.environ[key] = value
else:
    print(f"Warning: .env file not found at {env_path}")

SAFECUBE_API_KEY = os.getenv('SAFECUBE_API_KEY')
URL = 'https://api.sinay.ai/schedule/api/v1/schedules/port/traffic'
DAKAR_UNLOCODE = 'SNDKR'

def test_api():
    print(f"Testing API connection to: {URL}")
    print(f"API Key present: {'Yes' if SAFECUBE_API_KEY else 'No'}")
    
    if not SAFECUBE_API_KEY:
        print("ERROR: SAFECUBE_API_KEY not found in environment variables.")
        return

    headers = {
        'API_KEY': SAFECUBE_API_KEY,
        'Accept': 'application/json'
    }
    
    today = datetime.now().strftime('%Y-%m-%d')
    params = {
        'unlocode': DAKAR_UNLOCODE,
        'limit': 5, 
        'start_date': today,
        'end_date': today
    }
    
    print(f"Request parameters: {params}")
    
    try:
        response = requests.get(URL, headers=headers, params=params, timeout=10)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("Response JSON (first 500 chars):")
            print(json.dumps(data, indent=2)[:500] + "...")
            
            arrivals = data.get('data', {}).get('arrivals', [])
            departures = data.get('data', {}).get('departures', [])
            print(f"Arrivals count: {len(arrivals)}")
            print(f"Departures count: {len(departures)}")
        else:
            print(f"Error Response: {response.text}")
            
    except Exception as e:
        print(f"Exception occurred: {e}")

if __name__ == "__main__":
    test_api()
