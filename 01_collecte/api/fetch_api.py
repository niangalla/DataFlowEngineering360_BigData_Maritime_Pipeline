import requests
import pandas as pd
import json
import mysql.connector
from pymongo import MongoClient
import random

API_KEY = 'TA_CLÉ_API'  # Obtiens-la gratuitement sur api-sports.io
HEADERS = {'x-apisports-key': API_KEY}
URL = 'https://api-football.com/v3/players'  # Ex: Stats joueurs (adapte pour matchs via /fixtures ou /players?team=ID)

# Fetch exemple: Stats joueurs pour une équipe (e.g., Manchester United, ID=33)
params = {'team': 33, 'season': 2025}  # Saison actuelle
response = requests.get(URL, headers=HEADERS, params=params)
if response.status_code == 200:
    api_data = response.json()['response']  # Liste de joueurs avec stats
else:
    print("Erreur API:", response.status_code)
    api_data = []

# Normaliser en DataFrame (ex: extraire nom, âge, stats)
processed_data = []
for player in api_data:
    stats = player.get('statistics', [{}])[0]  # Première stats saison
    processed_data.append({
        'player_id': player['id'],
        'name': player['name'],
        'age': player['age'],
        'team': stats.get('team', {}).get('name'),
        'match_id': 'N/A',  # À adapter pour matchs
        'speed_kmh': random.uniform(20.0, 35.0),  # Simulé car pas toujours dispo; utilise si API a tracking
        'passes': stats.get('passes', {}).get('total', 0),
        'shots': stats.get('shots', {}).get('total', 0),
        'timestamp': player['lastUpdate'] if 'lastUpdate' in player else pd.Timestamp.now().isoformat()
    })

df = pd.DataFrame(processed_data)

# Stocker en fichiers plats
df.to_csv('api_players.csv', index=False)
with open('api_players.json', 'w') as f:
    json.dump(processed_data, f)

# Stocker en MySQL (même table que synthétique)
conn = mysql.connector.connect(host='localhost', user='root', password='password', database='sports_db')
cursor = conn.cursor()
for row in processed_data:
    cursor.execute('''INSERT IGNORE INTO players VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                   (str(row['player_id']), row['name'], row['age'], row['team'], row['match_id'],
                    row['speed_kmh'], row['passes'], row['shots'], row['timestamp']))
conn.commit()
conn.close()

# Stocker en MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['sports_db']
collection = db['players']
collection.insert_many(processed_data)
client.close()


print("Données API fetchées et stockées !")