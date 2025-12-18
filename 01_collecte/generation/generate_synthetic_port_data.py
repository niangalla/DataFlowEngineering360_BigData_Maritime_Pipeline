import pandas as pd
from faker import Faker
import random
import json
import os
# import mysql.connector
from pymongo import MongoClient

fake = Faker()

# Générer 100 enregistrements (trafic port : navires, cargaisons)
data = []
for _ in range(100):
    record = {
        'vessel_id': fake.uuid4(),
        'vessel_name': fake.company(),
        'arrival_time': fake.date_time_this_year().isoformat(),
        'cargo_type': random.choice(['Containers', 'Bulk', 'Oil', 'General']),
        'cargo_volume': random.randint(100, 10000),  # Tonnes
        'delay_minutes': random.randint(0, 120),  # Retards réalistes
        'port_zone': random.choice(['Nord', 'Sud', 'Ndayane'])
    }
    data.append(record)

df = pd.DataFrame(data)
# === Enregistrement en JSON Lines ===

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "../../02_source_donnees")

jsonl_path = os.path.join(OUTPUT_DIR, "synthetic_port_data.jsonl")

# Chaque ligne = un objet JSON (orient='records', lines=True)
df.to_json(jsonl_path, orient='records', lines=True, force_ascii=False)
# Chemin absolu du dossier du script


# os.makedirs(OUTPUT_DIR, exist_ok=True)

df.to_csv(os.path.join(OUTPUT_DIR, "synthetic_port_data.csv"), index=False)

# df.to_json(os.path.join(OUTPUT_DIR, "synthetic_port_data.jsonl"), orient='records')

# Stocker en fichiers
# df.to_csv("../../02_source_donnees/seances.csv", index=False)
# df.to_csv("../../02_source_donnees/synthetic_port_data.csv", index=False)
# df.to_json("../../02_source_donnees/synthetic_port_data.json", orient='records')

# Stocker en SQL (MySQL) - DB 'port_db'
# conn = mysql.connector.connect(host='localhost', user='root', password='password', database='port_db')
# cursor = conn.cursor()
# cursor.execute('''CREATE TABLE IF NOT EXISTS port_traffic (
#     vessel_id VARCHAR(36) PRIMARY KEY, vessel_name VARCHAR(255), arrival_time DATETIME,
#     cargo_type VARCHAR(50), cargo_volume INT, delay_minutes INT, port_zone VARCHAR(50)
# )''')
# for row in data:
#     cursor.execute('''INSERT IGNORE INTO port_traffic VALUES (%s, %s, %s, %s, %s, %s, %s)''',
#                    (row['vessel_id'], row['vessel_name'], row['arrival_time'], row['cargo_type'],
#                     row['cargo_volume'], row['delay_minutes'], row['port_zone']))
# conn.commit()
# conn.close()

# Stocker en NoSQL (MongoDB)
# client = MongoClient('mongodb://localhost:27017/')
# db = client['port_db']
# collection = db['port_traffic']
# collection.insert_many(data)
# client.close()

print("Données synthétiques générées et stockées !")