import os
import json
import time
from kafka import KafkaConsumer
from hdfs import InsecureClient
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = 'port_traffic'
HDFS_URL = os.getenv('HDFS_URL', 'http://localhost:9870')
HDFS_USER = os.getenv('HDFS_USER', 'root')
HDFS_PATH = '/datalake/raw/streaming'

def create_consumer():
    """Create and return a Kafka Consumer instance."""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='hdfs_archiver',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        return consumer
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return None

def get_hdfs_client():
    """Create and return an HDFS client."""
    try:
        client = InsecureClient(HDFS_URL, user=HDFS_USER)
        return client
    except Exception as e:
        print(f"Failed to connect to HDFS: {e}")
        return None

def main():
    consumer = create_consumer()
    hdfs_client = get_hdfs_client()
    
    if not consumer or not hdfs_client:
        return

    print(f"Starting consumer for topic '{KAFKA_TOPIC}'...")
    
    # Ensure HDFS directory exists
    try:
        hdfs_client.makedirs(HDFS_PATH)
    except Exception as e:
        print(f"Warning: Could not create directory (might exist): {e}")

    batch = []
    batch_size = 10  # Write every 10 records or timeout
    last_write_time = time.time()
    write_interval = 60 # seconds

    try:
        for message in consumer:
            record = message.value
            batch.append(record)
            
            current_time = time.time()
            
            if len(batch) >= batch_size or (current_time - last_write_time) > write_interval:
                if not batch:
                    continue
                    
                # Generate filename based on timestamp
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"{HDFS_PATH}/traffic_{timestamp}.jsonl"
                
                # Prepare data
                content = "\n".join([json.dumps(r) for r in batch])
                
                # Write to HDFS
                try:
                    with hdfs_client.write(filename, encoding='utf-8') as writer:
                        writer.write(content)
                    print(f"Wrote {len(batch)} records to {filename}")
                    batch = []
                    last_write_time = current_time
                except Exception as e:
                    print(f"Failed to write to HDFS: {e}")
                    # Keep batch to retry? Or drop? For now, we keep accumulating or retry next loop
                    # In production, handle DLQ
                    
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
