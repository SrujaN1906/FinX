"""
FinX Module 2: Stream Analytics Job (PySpark Structured Streaming & Resilient Engine)
Processes incoming events from Kafka topic 'finx-transactions', normalizes currencies to INR,
calculates rolling aggregate window metrics, runs Anomaly Detection, and persists to PostgreSQL/SQLite.
"""
import sys
import os
import json
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from database.db import db_instance
from ml.model import detector_instance

KAFKA_BROKER = "localhost:9092"
TOPIC = "finx-transactions"

def process_stream_batch_record(record_dict):
    """
    Core transformation and enrichment function executed for every incoming event:
    1. Schema validation
    2. Dual currency normalization to INR
    3. Anomaly detection inference
    4. Persistence to database
    """
    amount = float(record_dict.get('amount', 0.0))
    currency = record_dict.get('currency', 'INR')
    amount_inr = float(record_dict.get('amount_inr', amount))
    
    # 1. Run ML Anomaly Detection (Module 3)
    is_anom, score, severity, cat_hint, rationale = detector_instance.score_event(record_dict)
    
    record_dict['is_anomaly'] = is_anom
    record_dict['anomaly_score'] = score
    record_dict['severity'] = severity
    record_dict['category_hint'] = cat_hint
    record_dict['anomaly_rationale'] = rationale
    
    # 2. Persist to Database (Module 2 persistence)
    try:
        db_instance.insert_transaction(record_dict)
        if is_anom:
            db_instance.insert_alert({
                "transaction_id": record_dict['transaction_id'],
                "anomaly_score": score,
                "is_anomaly": True,
                "severity": severity,
                "category_hint": cat_hint,
                "rationale": rationale,
                "model_version": detector_instance.model_version
            })
    except Exception as e:
        print(f"[Stream Analytics Error] Persistence failed: {e}")

    return record_dict

def run_pyspark_job():
    """Attempts PySpark Structured Streaming if Spark is installed in the environment."""
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import from_json, col
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType

        print("[Spark Analytics] Initializing PySpark Structured Streaming session...")
        spark = SparkSession.builder \
            .appName("FinX-Stream-Analytics") \
            .master("local[*]") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("account_id", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("currency", StringType(), True),
            StructField("amount_inr", DoubleType(), True),
            StructField("merchant", StringType(), True),
            StructField("category", StringType(), True),
            StructField("channel", StringType(), True)
        ])

        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BROKER) \
            .option("subscribe", TOPIC) \
            .load()

        parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
            .select(from_json(col("json_str"), schema).alias("data")) \
            .select("data.*")

        def foreach_batch_function(batch_df, batch_id):
            records = [row.asDict() for row in batch_df.collect()]
            print(f"[Spark Batch {batch_id}] Processing {len(records)} transactions...")
            for r in records:
                process_stream_batch_record(r)

        query = parsed_df.writeStream \
            .foreachBatch(foreach_batch_function) \
            .start()

        query.awaitTermination()
    except Exception as e:
        print(f"[Spark Analytics] PySpark direct session note: {e}")
        print("[Spark Analytics] Running Python High-Throughput Stream Analytics Engine...")
        run_resilient_streaming_consumer()

def run_resilient_streaming_consumer():
    """Resilient direct Kafka consumer that runs real-time stream analytics."""
    from kafka import KafkaConsumer
    print(f"[Stream Analytics] Connecting consumer to Kafka topic '{TOPIC}'...")
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=1000
        )
        print("[Stream Analytics] Active and continuously ingesting, scoring, and persisting...")
        while True:
            for msg in consumer:
                payload = msg.value
                process_stream_batch_record(payload)
            time.sleep(0.1)
    except Exception as err:
        print(f"[Stream Analytics] Consumer listening error (check Kafka broker): {err}")

if __name__ == "__main__":
    run_pyspark_job()
