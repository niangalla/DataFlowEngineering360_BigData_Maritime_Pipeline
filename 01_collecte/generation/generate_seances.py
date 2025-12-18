import json, random
from datetime import datetime, timedelta, timezone
import numpy as np
import pandas as pd
from tqdm import tqdm
import os

# load sportifs
with open("../../02_source_donnees/sportifs.json") as f:
    sportifs = json.load(f)

SEANCE_TYPES = ["training", "match", "recovery", "long_run"]

def base_speed_kmh(sport, level):
    # basic baseline by sport & level
    base = {
        "Running": 10.0,
        "Cycling": 25.0,
        "Football": 7.0,
        "Basketball": 6.0,
        "Tennis": 5.5
    }.get(sport, 8.0)
    if level == "pro": base *= 1.25
    if level == "amateur": base *= 0.8
    return base

def gen_seance(sportif, seance_idx, now):
    sport = sportif['sport']
    level = sportif['level']
    base_speed = base_speed_kmh(sport, level)
    seance_type = random.choices(SEANCE_TYPES, weights=[50,20,15,10])[0]
    # duration & distance depend on seance type
    if seance_type == "training":
        duration_min = random.randint(30, 90)
        distance_km = round((base_speed * duration_min / 60.0) * random.uniform(0.8,1.1),2)
    elif seance_type == "match":
        duration_min = random.randint(60,120)
        distance_km = round((base_speed * duration_min / 60.0) * random.uniform(0.6,1.2),2)
    elif seance_type == "recovery":
        duration_min = random.randint(20,60)
        distance_km = round((base_speed * duration_min / 60.0) * random.uniform(0.4,0.7),2)
    else: # long_run
        duration_min = random.randint(90,240)
        distance_km = round((base_speed * duration_min / 60.0) * random.uniform(0.9,1.3),2)

    avg_speed = round(distance_km / (duration_min/60.0) if duration_min>0 else base_speed,2)
    max_speed = round(avg_speed * random.uniform(1.05,1.7),2)
    # heart rate: baseline depends on age and activity
    age = sportif['age']
    hr_rest = int(60 + (age-20)*0.2 + random.gauss(0,3))
    avg_hr = int(np.clip(hr_rest + (avg_speed/base_speed)*40 + random.gauss(0,5), 90, 190))
    max_hr = int(min(220-age, avg_hr + random.randint(5,40)))
    calories = int(distance_km * random.uniform(60,100)) if sport in ["Running","Cycling"] else int(duration_min * random.uniform(8,12))
    # fatigue score (0..1)
    fatigue = round(min(1.0, random.random() * (duration_min/120) + (avg_hr/200)*0.5), 2)
    # injury flag rare but possible when fatigue high
    injury = random.random() < (0.01 + 0.15*fatigue)

    timestamp = (now - timedelta(days=random.randint(0,14), hours=random.randint(0,23), minutes=random.randint(0,59))).replace(tzinfo=timezone.utc)

    # simple geo coords (simulate sessions around a city)
    lat = round(14.7 + random.uniform(-0.2,0.2),6)
    lon = round(-17.45 + random.uniform(-0.2,0.2),6)

    seance = {
        "sportif_id": sportif['sportif_id'],
        "seance_id": f"S{sportif['sportif_id']}_{seance_idx}",
        "timestamp": timestamp.isoformat(),
        "seance_type": seance_type,
        "duration_sec": duration_min*60,
        "distance_km": distance_km,
        "avg_speed_kmh": avg_speed,
        "max_speed_kmh": max_speed,
        "avg_heart_rate": avg_hr,
        "max_heart_rate": max_hr,
        "calories": calories,
        "lat": lat,
        "lon": lon,
        "fatigue_score": fatigue,
        "injury_flag": injury
    }
    return seance

def generate_seances(events_per_sportif=10):
    now = datetime.now(timezone.utc)
    seances = []
    for sportif in tqdm(sportifs):
        for i in range(events_per_sportif):
            seances.append(gen_seance(sportif, i+1, now))
    return seances

if __name__ == "__main__":
    # os.makedirs("output", exist_ok=True)
    seances = generate_seances(events_per_sportif=12)  # adjust
    # write JSONL
    with open("../../02_source_donnees/seances.jsonl","w") as f:
        for seance in seances:
            f.write(json.dumps(seance) + "\n")
    # write CSV for staging
    df = pd.DataFrame(seances)
    df.to_csv("../../02_source_donnees/seances.csv", index=False)
    # copy athletes reference
    # (expect athletes.json already present)
    print(f"Generation de {len(seances)} seances -> 02_source_donnees/")
