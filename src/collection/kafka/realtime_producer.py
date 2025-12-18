import asyncio
import websockets
import json
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

AISTREAM_KEY = os.getenv("AISTREAM_KEY")

async def connect_ais_stream():
    async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
        subscribe_message = {
            "APIKey": AISTREAM_KEY,
            "BoundingBoxes": [[[14.5, -17.5], [14.8, -17.3]]],  # Bbox pour Dakar
            "FilterMessageTypes": ["PositionReport"]  # Positions navires
        }
        await websocket.send(json.dumps(subscribe_message))

        async for message_json in websocket:
            message = json.loads(message_json)
            if message["MessageType"] == "PositionReport":
                ais_msg = message["Message"]["PositionReport"]
                print(f"[{datetime.now(timezone.utc)}] Vessel MMSI: {ais_msg['UserID']} "
                      f"Lat: {ais_msg['Latitude']} Lon: {ais_msg['Longitude']} Speed: {ais_msg['Sog']} Course: {ais_msg['Cog']}")

if __name__ == "__main__":
    asyncio.run(connect_ais_stream())