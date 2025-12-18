import asyncio
import json
import os
import websockets
from datetime import datetime, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()
AISSTREAM_API_KEY = "0b29975204a2f4644198bc62890b3782b6b7f46b"
# os.getenv("AISSTREAM_API_KEY")

producer = KafkaProducer(
    # bootstrap_servers="kafka:9092",
    bootstrap_servers= "localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=50,
    acks="1"
)

KAFKA_TOPIC = "port_stream"

SUBSCRIBE_MSG = {
    "APIKey": AISSTREAM_API_KEY,
    "BoundingBoxes": [[[0.0, -30.0], [30.0, 0.0]]],
    # [[[-90, -180], [90, 180]]],   # TEST : globe entier
    "FilterMessageTypes": ["PositionReport"]
    # ["PositionReport"]
}

PING_INTERVAL = 5   # ping every 20 seconds


async def connect_and_stream():

    while True:

        try:
            print("Connexion au WebSocket AISStream...")

            async with websockets.connect(
                "wss://stream.aisstream.io/v0/stream",
                ping_interval=None,        # désactive le ping auto (on gère manuellement)
                close_timeout=5
            ) as ws:

                print("Connecté ✔️ – Abonnement au flux…")
                await ws.send(json.dumps(SUBSCRIBE_MSG))

                # Task parallèle → keep-alive ping/pong
                async def ping_loop():
                    while True:
                        try:
                            await ws.ping()
                            # print("PING envoyé")
                        except:
                            break
                        await asyncio.sleep(PING_INTERVAL)

                asyncio.create_task(ping_loop())

                # Boucle message
                async for raw in ws:
                    msg = json.loads(raw)

                    if msg.get("MessageType") != "PositionReport":
                        continue

                    ais = msg["Message"]["PositionReport"]

                    record = {
                        "vessel_id": ais.get("UserID"),
                        "lat": ais.get("Latitude"),
                        "lon": ais.get("Longitude"),
                        "speed": ais.get("Sog"),
                        "course": ais.get("Cog"),
                        "nav_status": ais.get("NavStatus"),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }

                    print("📡 REÇU :", record)
                    producer.send(KAFKA_TOPIC, record)

        except Exception as e:
            print("❌ Erreur WebSocket :", e)
            print("🔄 Reconnexion dans 3 sec…")
            await asyncio.sleep(3)

# Lancer le process
asyncio.run(connect_and_stream())
