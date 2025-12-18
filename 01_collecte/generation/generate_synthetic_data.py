import pandas as pd
from faker import Faker
import random
import json
import mysql.connector
from pymongo import MongoClient

fake = Faker()

# Générer 100 enregistrements synthétiques (profils patients diabète)
data = []
for _ in range(20000):
    height = round(random.uniform(1.5, 2.0), 2)  # Taille en m (réaliste)
    weight = round(random.uniform(50.0, 120.0), 1)  # Poids en kg (réaliste)
    imc = round(weight / (height ** 2), 1)  # Calcul IMC
    patient = {
        'patient_id': fake.uuid4(),
        'name': fake.name(),
        'age': random.randint(30, 70),
        'gender': random.choice(['Male', 'Female']),
        'height': height,
        'weight': weight,
        'imc': imc,
        'blood_glucose': random.randint(70, 200),  # Glycémie mg/dL
        'insulin': random.randint(0, 50),  # Unités/jour
        'hypertension': random.choice([0, 1]),  # 1 pour oui
        'timestamp': fake.date_time_this_year().isoformat()
    }
    data.append(patient)

df = pd.DataFrame(data)

# Stocker en fichiers plats
df.to_csv('../../02_source_donnees/synthetic_patients.csv', index=False)  # CSV
df.to_json('../../02_source_donnees/synthetic_patients.json', orient='records')  # JSON

# Stocker en SQL (MySQL) - Assume DB 'diabetes_db' créée via Docker
conn = mysql.connector.connect(host='localhost', user='dbuser', password='passer123', database='diabetes_db')
cursor = conn.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS patients (
    patient_id VARCHAR(36) PRIMARY KEY, name VARCHAR(255), age INT, gender VARCHAR(10),
    imc FLOAT, blood_glucose INT, insulin INT, hypertension INT, timestamp DATETIME
)''')
for row in data:
    cursor.execute('''INSERT IGNORE INTO patients VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                   (row['patient_id'], row['name'], row['age'], row['gender'], row['imc'],
                    row['blood_glucose'], row['insulin'], row['hypertension'], row['timestamp']))
conn.commit()
conn.close()

# Stocker en NoSQL (MongoDB) - Assume collection 'patients' dans DB 'diabetes_db'
client = MongoClient('mongodb://localhost:27017/')
db = client['diabetes_db']
collection = db['patients']
collection.insert_many(data)
client.close()

print("Données synthétiques générées et stockées !")