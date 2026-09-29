import json
import time
import random
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer
from faker import Faker

# Initialize Faker and Kafka Producer
fake = Faker()
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'finx-transactions'

# Pre-define a pool of customers and merchants for consistency
account_ids = [f"ACC-{random.randint(1000, 1050)}" for _ in range(50)]
merchants = ["Agnel Cafeteria", "Millennium Book Stores", "Steam", "Shree Gokul Fast Food", "Amazon", "Uber"]
categories = ["Food & Dining", "Stationery", "Digital Credits", "Retail", "Transport"]
channels = ["POS", "Online", "ATM"]

print("Starting FinX Transaction Generator. Press Ctrl+C to stop.")

try:
    while True:
        # 95% chance of a normal transaction, 5% chance of an anomaly
        is_anomaly = random.random() < 0.05 
        
        transaction = {
            "transaction_id": str(uuid.uuid4()),
            "account_id": random.choice(account_ids),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "amount": round(random.uniform(500.0, 5000.0) if is_anomaly else random.uniform(5.0, 150.0), 2),
            "merchant": "Unknown Overseas Merchant" if is_anomaly else random.choice(merchants),
            "category": "Electronics" if is_anomaly else random.choice(categories),
            "channel": "Online" if is_anomaly else random.choice(channels)
        }
        
        # Publish to Kafka
        producer.send(TOPIC_NAME, transaction)
        
        if is_anomaly:
            print(f"⚠️ INJECTED ANOMALY: {transaction['amount']} at {transaction['merchant']}")
        else:
            print(f"Published normal transaction: {transaction['transaction_id']}")
            
        # Control the stream rate (e.g., 1 transaction every 1 to 3 seconds)
        time.sleep(random.uniform(1.0, 3.0))

except KeyboardInterrupt:
    print("\nGenerator stopped.")
finally:
    producer.close()
