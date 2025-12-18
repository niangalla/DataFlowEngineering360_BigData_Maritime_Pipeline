import asyncio
import websockets
import json
import os
from datetime import datetime
from threading import Thread
from flask import Flask, jsonify
from dotenv import load_dotenv
from kafka import KafkaProducer
from pymongo import MongoClient
from sqlalchemy import create_engine
import pandas as pd

load_dotenv()

# Configuration
AISSTREAM_KEY = os.getenv("AISTREAM_KEY")

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'port-vessels-realtime')

# Flask app
app = Flask(__name__)

# Kafka Producer
try:
    kafka_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(f"Kafka producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
except Exception as e:
    print(f"Kafka connection failed: {e}")
    kafka_producer = None

# Stats
vessel_count = 0
last_message_time = None

# Dakar bounding box
DAKAR_BBOX = [[14.5, -17.5], [14.8, -17.3]]


def send_to_kafka(vessel_data):
    """Send vessel data to Kafka topic"""
    if kafka_producer:
        try:
            kafka_producer.send(KAFKA_TOPIC, value=vessel_data)
            kafka_producer.flush()
        except Exception as e:
            print(f"Kafka send error: {e}")

async def connect_ais_stream():
    """Connect to AISStream WebSocket and process messages"""
    global vessel_count, last_message_time
    
    async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
        subscribe_message = {
            "APIKey": AISSTREAM_KEY,
            "BoundingBoxes": [DAKAR_BBOX],
            "FilterMessageTypes": ["PositionReport"]
        }
        
        await websocket.send(json.dumps(subscribe_message))
        print(f"Subscribed to AISStream for Dakar port")
        print(f"Bounding box: {DAKAR_BBOX}")
        print(f"Kafka topic: {KAFKA_TOPIC}")
        
        async for message_json in websocket:
            try:
                message = json.loads(message_json)
                
                if message.get("MessageType") == "PositionReport":
                    ais_msg = message["Message"]["PositionReport"]
                    metadata = message.get("MetaData", {})
                    
                    vessel_data = {
                        "mmsi": ais_msg.get("UserID"),
                        "vessel_name": metadata.get("ShipName", "Unknown"),
                        "latitude": ais_msg.get("Latitude"),
                        "longitude": ais_msg.get("Longitude"),
                        "speed": ais_msg.get("Sog"),
                        "course": ais_msg.get("Cog"),
                        "heading": ais_msg.get("TrueHeading"),
                        "navigational_status": ais_msg.get("NavigationalStatus"),
                        "position_accuracy": ais_msg.get("PositionAccuracy"),
                        "rate_of_turn": ais_msg.get("RateOfTurn"),
                        "timestamp": metadata.get("time_utc", datetime.utcnow().isoformat()),
                        "received_at": datetime.utcnow().isoformat()
                    }
                    
                    # Send to Kafka (priority for streaming pipeline)
                    send_to_kafka(vessel_data)
                    
                    # Update stats
                    vessel_count += 1
                    last_message_time = datetime.utcnow()
                    
                    print(f"[{vessel_count}] MMSI: {vessel_data['mmsi']} | "
                          f"Pos: ({vessel_data['latitude']:.5f}, {vessel_data['longitude']:.5f}) | "
                          f"Speed: {vessel_data['speed']:.1f} knots | "
                          f"Course: {vessel_data['course']:.0f}°")
                    
            except Exception as e:
                print(f"Error processing message: {e}")


def run_aisstream_background():
    """Run AISStream connection in background thread"""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(connect_ais_stream())


@app.route('/health')
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "running",
        "vessel_count": vessel_count,
        "last_message": last_message_time.isoformat() if last_message_time else None,
        "kafka_connected": kafka_producer is not None,
        "timestamp": datetime.utcnow().isoformat()
    })


@app.route('/stats')
def stats():
    """Statistics endpoint"""
    return jsonify({
        "total_messages_processed": vessel_count,
        "last_message_time": last_message_time.isoformat() if last_message_time else None,
        "kafka_topic": KAFKA_TOPIC,
        "kafka_servers": KAFKA_BOOTSTRAP_SERVERS,
        "monitoring_area": DAKAR_BBOX,
        "timestamp": datetime.utcnow().isoformat()
    })


def main():
    if not AISSTREAM_KEY:
        print("Error: AISSTREAM_KEY not found in .env")
        return
    
    print("="*60)
    print("AISStream Real-Time Data Pipeline")
    print("="*60)
    print(f"Monitoring: Dakar Port {DAKAR_BBOX}")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS} -> {KAFKA_TOPIC}")
    print(f"API: http://localhost:5000/health")
    print("="*60)
    
    # Start AISStream connection in background
    thread = Thread(target=run_aisstream_background, daemon=True)
    thread.start()
    
    # Start Flask API
    app.run(host='0.0.0.0', port=5000, debug=False)

if __name__ == "__main__":
    main()