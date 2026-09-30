import csv
import time
import json
import os
import argparse
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def main():
    parser = argparse.ArgumentParser(description="Replay CSV dataset to Kafka")
    parser.add_argument('--dataset', type=str, choices=['huge', 'controlled'], default='controlled',
                        help="Which dataset to replay: 'huge' (10,000 rows) or 'controlled' (10 rows)")
    args = parser.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    filename = 'transactions_controlled.csv' if args.dataset == 'controlled' else 'transactions_huge.csv'
    csv_file_path = os.path.join(project_root, 'data', filename)
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=json_serializer
        )
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        print("Please ensure Docker Desktop is running and you have run 'docker-compose up -d'")
        return
        
    topic = 'finx-transactions'
    
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            data = list(reader)
    except FileNotFoundError:
        print(f"Error: Could not find {csv_file_path}")
        return

    print(f"Starting to replay '{filename}' ({len(data)} rows) to topic '{topic}'...")

    for row in data:
        # Construct the payload according to the Blueprint schema
        payload = {
            "transaction_id": row['transaction_id'],
            "account_id": row['account_id'],
            "timestamp": row['timestamp'],
            "amount": float(row['amount']),
            "merchant": row['merchant'],
            "category": row['category'],
            "channel": row['channel']
        }
        
        is_anomaly = row.get('is_anomaly_label') == 'Yes'
        
        # Publish the event to the topic
        producer.send(topic, value=payload)
        
        if is_anomaly:
            print(f"⚠️ INJECTED ANOMALY (Controlled): {payload['amount']} at {payload['merchant']}")
        else:
            print(f"Published normal transaction: {payload['transaction_id']}")
            
        # Simulate real-time stream (faster for huge, slower for controlled)
        delay = 1.0 if args.dataset == 'controlled' else 0.1
        time.sleep(delay)

    producer.flush()
    print("Finished dataset replay.")

if __name__ == '__main__':
    main()
