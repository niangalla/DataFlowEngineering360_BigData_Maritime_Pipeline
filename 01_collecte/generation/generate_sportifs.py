import json
import random
from faker import Faker
fake = Faker()

SPORTS = ["Football", "Running", "Cycling", "Basketball", "Tennis"]
LEVELS = ["amateur", "club", "semi-pro", "pro"]

def gen_sportif(i):
    age = random.randint(15, 45)
    sport = random.choice(SPORTS)
    # weight/height reasonable by sport
    if sport == "Cycling":
        weight = round(random.uniform(60, 78),1)
    elif sport == "Running":
        weight = round(random.uniform(55, 75),1)
    elif sport == "Basketball":
        weight = round(random.uniform(75, 100),1)
    else:
        weight = round(random.uniform(60, 90),1)
    height = random.randint(160,200)
    return {
        "sportif_id": f"S{str(i).zfill(2)}",
        "name": fake.name(),
        "age": age,
        "sex": random.choice(["M","F"]),
        "team": fake.company() if random.random() < 0.6 else None,
        "sport": sport,
        "weight_kg": weight,
        "height_cm": height,
        "level": random.choices(LEVELS, weights=[50,30,15,5])[0]
    }

if __name__ == "__main__":
    N = 20000
    sportifs = [gen_sportif(i+1) for i in range(N)]
    with open("../../02_source_donnees/sportifs.json", "w") as f:
        json.dump(sportifs, f, indent=2)
    print(f"Generation de {N} sportifs -> 02_source_donnees/sportifs.json")
