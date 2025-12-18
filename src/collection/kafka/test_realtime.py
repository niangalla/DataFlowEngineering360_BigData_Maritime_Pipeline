import asyncio
import websockets
import json

async def test_ais_stream():
    async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
        subscribe_message = {
            "APIKey": "0b29975204a2f4644198bc62890b3782b6b7f46b",
            "BoundingBoxes": [[[13.0, -18.5], [16.0, -16.0]]],  # Élargi pour Singapour
            "FilterMessageTypes": ["PositionReport"]
        }
        await websocket.send(json.dumps(subscribe_message))

        async for message_json in websocket:
            message = json.loads(message_json)
            print(message)  # Print pour debug

asyncio.run(test_ais_stream())