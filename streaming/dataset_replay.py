import pandas as pd
import time
import json
import uuid
import os
from datetime import datetime, timezone
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def main():
    # Construct the file path to the CSV data
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    csv_file_path = os.path.join(project_root, 'data', 'transactions.csv')
    
    # Initialize KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=json_serializer
    )
    
    topic = 'finx-transactions'
    
    try:
        # Read the CSV data using pandas
        df = pd.read_csv(csv_file_path)
    except FileNotFoundError:
        print(f"Error: Could not find {csv_file_path}")
        return

    print(f"Starting to replay data to topic '{topic}'...")

    for _, row in df.iterrows():
        # Map the data into the strict JSON dictionary schema
        payload = {
            "transaction_id": str(uuid.uuid4()),
            "account_id": row['account_id'],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "amount": float(row['amount']),
            "merchant": row['merchant'],
            "category": row['category'],
            "channel": row['channel']
        }
        
        # Publish the event to the topic
        producer.send(topic, value=payload)
        print(f"Published: {payload}")
        
        # Simulate real-time stream
        time.sleep(0.5)

    producer.flush()
    print("Finished dataset replay.")

if __name__ == '__main__':
    main()
