"""
FinX Real-Time Ingestion & Demo Server
Provides REST and SSE endpoints for Kafka streaming visualization and replaying datasets.
"""
import sys
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

import os
import csv
import json
import time
import queue
import threading
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from kafka import KafkaProducer, KafkaConsumer

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FRONTEND_DIR = os.path.join(PROJECT_ROOT, "frontend")
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
TOPIC = "finx-transactions"
BOOTSTRAP_SERVERS = ["localhost:9092"]

# Event broadcast queue for connected SSE clients
subscribers = []
subscribers_lock = threading.Lock()
recent_events = []
recent_events_lock = threading.Lock()
active_replay = {"running": False, "thread": None}

def broadcast_event(event_dict):
    with recent_events_lock:
        recent_events.append(event_dict)
        if len(recent_events) > 200:
            recent_events.pop(0)

    data_str = json.dumps(event_dict)
    dead_subs = []
    with subscribers_lock:
        for q in subscribers:
            try:
                q.put_nowait(data_str)
            except Exception:
                dead_subs.append(q)
        for d in dead_subs:
            if d in subscribers:
                subscribers.remove(d)

def kafka_listener_worker():
    """Background thread that consumes from Kafka and broadcasts to SSE clients."""
    print("[Kafka Listener] Starting listener on topic:", TOPIC)
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset='latest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=1000
            )
            print("[Kafka Listener] Connected successfully to Kafka broker.")
            for message in consumer:
                payload = message.value
                payload['_kafka_partition'] = message.partition
                payload['_kafka_offset'] = message.offset
                payload['_received_at'] = time.time()
                broadcast_event(payload)
        except Exception as e:
            # Reconnect loop
            time.sleep(3)

def replay_worker(dataset_name, delay=0.8):
    active_replay["running"] = True
    filename = 'transactions_controlled.csv' if dataset_name == 'controlled' else 'transactions_huge.csv'
    csv_file_path = os.path.join(DATA_DIR, filename)

    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda d: json.dumps(d).encode('utf-8')
        )
    except Exception as e:
        broadcast_event({"type": "system_log", "level": "error", "message": f"Kafka connection failed: {e}"})
        active_replay["running"] = False
        return

    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception as e:
        broadcast_event({"type": "system_log", "level": "error", "message": f"CSV read failed: {e}"})
        active_replay["running"] = False
        return

    broadcast_event({
        "type": "replay_started",
        "dataset": dataset_name,
        "total_rows": len(rows),
        "filename": filename
    })

    for i, row in enumerate(rows):
        if not active_replay["running"]:
            break

        is_anomaly = (row.get('is_anomaly_label') == 'Yes') or (float(row['amount']) > 5000.0)

        payload = {
            "transaction_id": row['transaction_id'],
            "account_id": row['account_id'],
            "timestamp": row['timestamp'],
            "amount": float(row['amount']),
            "merchant": row['merchant'],
            "category": row['category'],
            "channel": row['channel'],
            "is_anomaly": is_anomaly,
            "index": i + 1,
            "total": len(rows),
            "dataset": dataset_name
        }

        producer.send(TOPIC, value=payload)
        broadcast_event(payload)
        time.sleep(delay)

    producer.flush()
    broadcast_event({
        "type": "replay_finished",
        "dataset": dataset_name,
        "total_rows": len(rows)
    })
    active_replay["running"] = False

class FinXHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=FRONTEND_DIR, **kwargs)

    def do_GET(self):
        parsed = urlparse(self.path)
        
        if parsed.path == "/api/status":
            self.handle_api_status()
        elif parsed.path == "/api/history":
            self.handle_api_history()
        elif parsed.path == "/api/events":
            self.handle_sse()
        else:
            # Fallback to static files
            super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/replay":
            params = parse_qs(parsed.query)
            dataset = params.get('dataset', ['controlled'])[0]
            try:
                speed = float(params.get('speed', [1.0])[0])
            except Exception:
                speed = 1.0

            content_len = int(self.headers.get('Content-Length', 0))
            if content_len > 0:
                try:
                    body_bytes = self.rfile.read(content_len)
                    body_json = json.loads(body_bytes.decode('utf-8'))
                    if 'dataset' in body_json:
                        dataset = body_json['dataset']
                    if 'speed' in body_json:
                        speed = float(body_json['speed'])
                except Exception:
                    pass

            delay = 0.8 / speed if dataset == 'controlled' else 0.05 / speed
            
            if active_replay["running"]:
                active_replay["running"] = False
                time.sleep(0.3)

            t = threading.Thread(target=replay_worker, args=(dataset, delay), daemon=True)
            active_replay["thread"] = t
            t.start()

            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "started", "dataset": dataset}).encode('utf-8'))
        elif parsed.path == "/api/stop":
            active_replay["running"] = False
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "stopped"}).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

    def handle_api_status(self):
        kafka_ok = False
        message_count = 0
        try:
            c = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                consumer_timeout_ms=1000
            )
            kafka_ok = True
            c.close()
        except Exception:
            kafka_ok = False

        status = {
            "kafka_connected": kafka_ok,
            "broker": BOOTSTRAP_SERVERS[0],
            "topic": TOPIC,
            "active_replay": active_replay["running"],
            "recent_count": len(recent_events)
        }
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(status).encode('utf-8'))

    def handle_api_history(self):
        with recent_events_lock:
            events = list(recent_events)
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps({"events": events}).encode('utf-8'))

    def handle_sse(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('Cache-Control', 'no-cache')
        self.send_header('Connection', 'keep-alive')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        client_queue = queue.Queue(maxsize=100)
        with subscribers_lock:
            subscribers.append(client_queue)

        # Send initial connected event
        try:
            self.wfile.write(f"data: {json.dumps({'type': 'connected', 'topic': TOPIC})}\n\n".encode('utf-8'))
            self.wfile.flush()
        except Exception:
            return

        try:
            while True:
                try:
                    data_str = client_queue.get(timeout=15.0)
                    self.wfile.write(f"data: {data_str}\n\n".encode('utf-8'))
                    self.wfile.flush()
                except queue.Empty:
                    # Keepalive ping
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
        except Exception:
            pass
        finally:
            with subscribers_lock:
                if client_queue in subscribers:
                    subscribers.remove(client_queue)

def run_server(port=8080):
    os.chdir(FRONTEND_DIR)
    listener_thread = threading.Thread(target=kafka_listener_worker, daemon=True)
    listener_thread.start()

    server_address = ('', port)
    httpd = ThreadingHTTPServer(server_address, FinXHandler)
    print(f"[FinX Server] Running at http://localhost:{port}/")
    print(f"[FinX Server] Serving UI from: {FRONTEND_DIR}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[FinX Server] Shutting down.")
        httpd.server_close()

if __name__ == '__main__':
    port = 8080
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    run_server(port)
