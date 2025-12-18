from kafka import KafkaProducer
import json
import pandas as pd

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
TOPIC = 'port_stream_traffic'

# Lis fetché (assume après run fetch script)
df = pd.read_csv("../../02_source_donnees/historical_marinetraffic.csv")

# Publie
for _, row in df.iterrows():
    producer.send(TOPIC, row.to_dict())
producer.flush()

print(f"Publié {len(df)} historical escales en Kafka !")