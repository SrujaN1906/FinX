"""
FinX Kafka Consumer Monitor (CLI Demo)
Listens to 'finx-transactions' topic and displays incoming events with schema validation and anomaly highlights.
"""
import sys
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

import json
import time
from kafka import KafkaConsumer

REQUIRED_FIELDS = ["transaction_id", "account_id", "timestamp", "amount", "merchant", "category", "channel"]

def format_currency(val):
    try:
        return f"${float(val):,.2f}"
    except Exception:
        return str(val)

def main():
    print("=" * 70)
    print("   FinX Real-Time Ingestion Monitor - Kafka Topic: finx-transactions   ")
    print("=" * 70)
    print("Connecting to Kafka at localhost:9092...")
    
    try:
        consumer = KafkaConsumer(
            'finx-transactions',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=1000
        )
    except Exception as e:
        print(f"[ERROR] Could not connect to Kafka: {e}")
        print("Please ensure docker-compose is running ('docker-compose up -d').")
        return

    print("[CONNECTED] Listening for real-time transactions... (Press Ctrl+C to stop)\n")
    
    total = 0
    anomalies = 0

    try:
        while True:
            for message in consumer:
                total += 1
                data = message.value
                
                # Check Schema validation
                valid_schema = all(k in data for k in REQUIRED_FIELDS)
                schema_badge = "✓ VALID SCHEMA" if valid_schema else "✗ INVALID SCHEMA"
                
                # Detect if anomaly (based on amount threshold or high value for demo)
                amount = float(data.get("amount", 0))
                is_anomaly = amount > 1000.0 or "Unknown" in data.get("merchant", "")
                
                if is_anomaly:
                    anomalies += 1
                    print("-" * 70)
                    print(f"⚠️  [ALERT - SEEDED ANOMALY DETECTED] #{total}")
                    print(f"   Txn ID:    {data.get('transaction_id')}")
                    print(f"   Account:   {data.get('account_id')}")
                    print(f"   Timestamp: {data.get('timestamp')}")
                    print(f"   Amount:    {format_currency(amount)} [HIGH VALUE EXTREME]")
                    print(f"   Merchant:  {data.get('merchant')}")
                    print(f"   Category:  {data.get('category')} | Channel: {data.get('channel')}")
                    print(f"   Status:    {schema_badge} | Kafka Partition: {message.partition} Off: {message.offset}")
                    print("-" * 70 + "\n")
                else:
                    print(f"[TXN #{total:03d}] {data.get('transaction_id')[:8]}... | {data.get('account_id')} | {format_currency(amount):>9} | {data.get('merchant'):<24} | {data.get('channel'):<6} [{schema_badge}]")
            
            time.sleep(0.1)
    except KeyboardInterrupt:
        print(f"\n[STOPPED] Session Summary: {total} transactions monitored, {anomalies} anomalies detected.")

if __name__ == '__main__':
    main()
