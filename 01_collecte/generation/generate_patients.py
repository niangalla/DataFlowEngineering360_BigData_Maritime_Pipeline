import json
import csv
from faker import Faker
from datetime import datetime, timezone
import random
import time

fake = Faker()

patients = [f"P{str(i).zfill(3)}" for i in range(1, 21)]

def gen_donnee_patient():
    return {
        "patient_id": random.choice(patients),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature_c": round(random.normalvariate(36.7, 0.6), 1),
        "heart_rate": random.randint(88, 100),
        "spo2": random.randint(88, 100),
        "lat": round(random.uniform(14.6, 14.8), 6),
        "lon": round(random.uniform(-17.5, -17.3), 6) 
    }

def write_csv(path, rows):
    keys = rows[0].keys()
    with open(path, "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)

def write_jsonline(path, rows):
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")

if __name__ == "__main__":
    #Generer 10 mil exemples
    rows = [gen_donnee_patient() for _ in range(10000)]
    write_csv("../../02_stockage_temporaire/donnee_vitales_patients.csv", rows)
    write_jsonline("../../02_stockage_temporaire/donnee_vitales_patients.jsonl", rows)
    print("Fichiers crées avec succes")