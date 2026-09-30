import csv
import random
import uuid
from datetime import datetime, timezone, timedelta
from faker import Faker
import os

fake = Faker()

def generate_transactions(num_rows, is_controlled=False):
    transactions = []
    account_ids = [f"ACC-{random.randint(1000, 1050)}" for _ in range(50)]
    merchants = ["Agnel Cafeteria", "Millennium Book Stores", "Steam", "Shree Gokul Fast Food", "Amazon", "Uber"]
    categories = ["Food & Dining", "Stationery", "Digital Credits", "Retail", "Transport"]
    channels = ["POS", "Online", "ATM"]
    
    base_time = datetime.now(timezone.utc) - timedelta(days=1)
    
    for i in range(num_rows):
        is_anomaly = False
        if is_controlled and i == 7: # The 8th transaction in controlled data is an anomaly
            is_anomaly = True
            
        if not is_controlled and random.random() < 0.05:
            is_anomaly = True
            
        t_time = base_time + timedelta(minutes=i*2)
        
        transaction = {
            "transaction_id": str(uuid.uuid4()),
            "account_id": random.choice(account_ids),
            "timestamp": t_time.isoformat(),
            "amount": round(random.uniform(5000.0, 15000.0) if is_anomaly else random.uniform(5.0, 150.0), 2),
            "merchant": "Unknown Overseas Merchant" if is_anomaly else random.choice(merchants),
            "category": "Electronics" if is_anomaly else random.choice(categories),
            "channel": "Online" if is_anomaly else random.choice(channels),
            "is_anomaly_label": "Yes" if is_anomaly else "No"  # Only for our reference, wouldn't be sent to Kafka
        }
        transactions.append(transaction)
    return transactions

def save_csv(filename, data):
    if not data: return
    keys = data[0].keys()
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        dict_writer = csv.DictWriter(f, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, 'data')
    
    huge_data = generate_transactions(10000)
    controlled_data = generate_transactions(10, is_controlled=True)
    
    save_csv(os.path.join(data_dir, 'transactions_huge.csv'), huge_data)
    save_csv(os.path.join(data_dir, 'transactions_controlled.csv'), controlled_data)
    
    print("CSVs generated successfully.")

if __name__ == '__main__':
    main()
