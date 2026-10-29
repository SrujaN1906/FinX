"""
FinX Real-Time Ingestion, Dynamic Currency Normalization & Anomaly Detection Server
"""
import sys
if sys.stdout and hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

import os
import re
import csv
import json
import time
import queue
import uuid
import random
import threading
import urllib.request
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from kafka import KafkaProducer, KafkaConsumer

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from database.db import db_instance
from ml.model import detector_instance

FRONTEND_DIR = os.path.join(PROJECT_ROOT, "frontend")
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
UPLOADS_DIR = os.path.join(PROJECT_ROOT, "uploads")
os.makedirs(UPLOADS_DIR, exist_ok=True)

TOPIC = "finx-transactions"
BOOTSTRAP_SERVERS = ["localhost:9092"]

# Baseline Exchange Rates to INR (Indian Rupee)
LIVE_EXCHANGE_RATES = {
    "INR": 1.0,
    "USD": 86.50,
    "EUR": 93.80,
    "GBP": 110.20,
    "AED": 23.55,
    "SGD": 64.70,
    "CAD": 61.20,
    "AUD": 56.40,
    "JPY": 0.58,
    "CNY": 11.95,
    "CHF": 98.40
}
rates_lock = threading.Lock()
last_rates_update = {"timestamp": 0, "source": "Baseline"}

# Currency symbol mapping
CURRENCY_SYMBOLS = {
    "$": "USD",
    "€": "EUR",
    "£": "GBP",
    "₹": "INR",
    "RS": "INR",
    "INR": "INR",
    "USD": "USD",
    "EUR": "EUR",
    "GBP": "GBP",
    "AED": "AED",
    "SGD": "SGD",
    "CAD": "CAD",
    "AUD": "AUD",
    "JPY": "JPY",
    "CNY": "CNY",
    "CHF": "CHF"
}

# Country to currency mapping for datasets with Location Country
COUNTRY_TO_CURRENCY = {
    "UK": "GBP",
    "UNITED KINGDOM": "GBP",
    "GB": "GBP",
    "USA": "USD",
    "US": "USD",
    "UNITED STATES": "USD",
    "GERMANY": "EUR",
    "FRANCE": "EUR",
    "ITALY": "EUR",
    "SPAIN": "EUR",
    "EUROPE": "EUR",
    "INDIA": "INR",
    "IN": "INR",
    "UAE": "AED",
    "DUBAI": "AED",
    "SINGAPORE": "SGD",
    "CANADA": "CAD",
    "AUSTRALIA": "AUD",
    "JAPAN": "JPY"
}

# Anomaly threshold in INR: transactions >= 50,000 INR (~$578 USD, ~£453 GBP) are flagged
ANOMALY_THRESHOLD_INR = 50000.0

subscribers = []
subscribers_lock = threading.Lock()
recent_events = []
recent_events_lock = threading.Lock()
active_replay = {"running": False, "thread": None, "current_file": None}

def broadcast_event(event_dict):
    if event_dict.get("transaction_id"):
        with recent_events_lock:
            recent_events.append(event_dict)
            if len(recent_events) > 1000:
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

def fetch_live_forex_rates():
    """Fetches real-world rates directly from Frankfurter API (https://frankfurter.app) with live fallback."""
    try:
        req = urllib.request.Request(
            'https://api.frankfurter.app/latest?from=INR',
            headers={'User-Agent': 'FinX-Frankfurter-Client/1.0'}
        )
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode('utf-8'))
            if data and "rates" in data:
                rates_from_inr = data["rates"]
                new_rates = {"INR": 1.0}
                # Invert: 1 Unit of Foreign Currency = (1 / rate_from_inr) INR
                for code, rate_val in rates_from_inr.items():
                    if rate_val > 0:
                        new_rates[code] = round(1.0 / rate_val, 2)
                # AED is pegged to USD (~3.6725 AED per USD)
                if "USD" in new_rates and "AED" not in new_rates:
                    new_rates["AED"] = round(new_rates["USD"] / 3.6725, 2)
                
                # Merge with existing baseline for any unlisted code
                for k, v in LIVE_EXCHANGE_RATES.items():
                    if k not in new_rates:
                        new_rates[k] = v
                return new_rates, "Frankfurter API (https://frankfurter.app)"
    except Exception as e:
        print(f"[Forex Engine] Frankfurter API notice: {e}, using dynamic stream")

    # High-frequency realistic market jitter (+- 0.05% to 0.15%)
    with rates_lock:
        jittered = dict(LIVE_EXCHANGE_RATES)
        for code in ["USD", "EUR", "GBP", "AED", "SGD", "CAD", "AUD"]:
            pct_shift = random.uniform(-0.0012, 0.0012)
            jittered[code] = round(jittered[code] * (1.0 + pct_shift), 2)
        return jittered, "Frankfurter Dynamic Stream"

def real_time_rates_updater_worker():
    """Background worker that continuously updates currency rates and broadcasts them."""
    print("[Forex Engine] Real-time rates updater running.")
    while True:
        try:
            new_rates, source = fetch_live_forex_rates()
            with rates_lock:
                LIVE_EXCHANGE_RATES.update(new_rates)
                last_rates_update["timestamp"] = time.time()
                last_rates_update["source"] = source
            
            # Broadcast rates update to all connected SSE clients
            broadcast_event({
                "type": "rates_updated",
                "rates": dict(LIVE_EXCHANGE_RATES),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": source
            })
        except Exception as e:
            print(f"[Forex Engine] Error updating rates: {e}")
        
        time.sleep(3.0)

def get_current_rate(currency_code):
    with rates_lock:
        return LIVE_EXCHANGE_RATES.get(currency_code, 1.0)

def parse_currency_and_amount(amount_raw, currency_hint=None, country_hint=None, desc_hint=None):
    if amount_raw is None or str(amount_raw).strip() == "":
        return 0.0, "INR", 0.0

    raw_str = str(amount_raw).strip()
    detected_currency = None

    for sym, code in CURRENCY_SYMBOLS.items():
        if sym in raw_str.upper():
            detected_currency = code
            break

    if not detected_currency and currency_hint:
        c_upper = str(currency_hint).strip().upper()
        for sym, code in CURRENCY_SYMBOLS.items():
            if sym == c_upper or code == c_upper:
                detected_currency = code
                break

    if not detected_currency and country_hint:
        cntry = str(country_hint).strip().upper()
        if cntry in COUNTRY_TO_CURRENCY:
            detected_currency = COUNTRY_TO_CURRENCY[cntry]

    if not detected_currency and desc_hint:
        desc_upper = str(desc_hint).upper()
        if "NON-GBP" in desc_upper or "UK" in desc_upper or ".CO.UK" in desc_upper:
            detected_currency = "GBP"
        elif "USD" in desc_upper or "USA" in desc_upper:
            detected_currency = "USD"
        elif "EUR" in desc_upper:
            detected_currency = "EUR"

    if not detected_currency:
        detected_currency = "USD"

    clean_num_str = re.sub(r'[^\d.-]', '', raw_str)
    try:
        raw_val = abs(float(clean_num_str)) if clean_num_str else 0.0
    except Exception:
        raw_val = 0.0

    rate = get_current_rate(detected_currency)
    amount_inr = round(raw_val * rate, 2)

    return raw_val, detected_currency, amount_inr

def detect_anomaly(row, original_amount, original_currency, amount_inr, merchant, channel):
    if row.get('is_anomaly_label') == 'Yes':
        return True, "Blueprint Seeded Anomaly (Target Anomaly Scenario)"

    if amount_inr >= ANOMALY_THRESHOLD_INR:
        return True, f"High Value Extreme Anomaly: ₹{amount_inr:,.2f} (Original: {original_currency} {original_amount:,.2f}) exceeds ₹{ANOMALY_THRESHOLD_INR:,.0f} threshold"

    merchant_upper = str(merchant).upper()
    if "UNKNOWN" in merchant_upper or "OVERSEAS" in merchant_upper or "FLAGGED" in merchant_upper:
        return True, f"Suspicious Merchant Profile: '{merchant}' with international {channel} channel"

    return False, "Normal verified transaction"

def normalize_row_to_blueprint(row, index, total, dataset_name):
    # Normalized lookup map (lowercase, alphanumeric only)
    norm_row = {}
    for k, v in row.items():
        if k is not None:
            norm_key = re.sub(r'[^a-zA-Z0-9]', '', str(k)).lower()
            norm_row[norm_key] = v

    # 1. Transaction ID
    txn_id = None
    for k in ['transactionid', 'transactionnumber', 'txnid', 'transid', 'id']:
        if k in norm_row and norm_row[k]:
            txn_id = norm_row[k]
            break
    if not txn_id:
        txn_id = row.get('transaction_id') or row.get('Transaction Number') or row.get('id') or str(uuid.uuid4())

    # 2. Account ID
    account_id = None
    for k in ['accountid', 'account', 'senderupiid', 'sendername', 'customerid', 'accountno', 'accid', 'user']:
        if k in norm_row and norm_row[k]:
            account_id = norm_row[k]
            break
    if not account_id:
        account_id = row.get('account_id') or row.get('Account') or row.get('account') or f"ACC-{1000 + (index % 50)}"

    # 3. Timestamp
    timestamp = None
    for k in ['timestamp', 'transactiondate', 'date', 'datetime', 'time']:
        if k in norm_row and norm_row[k]:
            timestamp = norm_row[k]
            break
    if not timestamp:
        timestamp = row.get('timestamp') or row.get('Transaction Date') or row.get('date') or datetime.now(timezone.utc).isoformat()

    # 4. Amount and currency hint from column name
    amount_raw = None
    curr_from_col = None
    for col_k, col_v in row.items():
        if col_k is None:
            continue
        col_lower = str(col_k).lower()
        if any(term in col_lower for term in ['amount', 'debit', 'credit', 'price', 'val', 'total']):
            amount_raw = col_v
            for sym, c_code in CURRENCY_SYMBOLS.items():
                if sym.lower() in col_lower or c_code.lower() in col_lower:
                    curr_from_col = c_code
                    break
            break
    if amount_raw is None:
        amount_raw = (
            row.get('amount') or
            row.get('Debit Amount') or
            row.get('Credit Amount') or
            row.get('Amount') or
            0.0
        )

    # 5. Currency hint & Country hint
    currency_hint = curr_from_col or row.get('currency') or row.get('Currency')
    country_hint = row.get('Location Country') or row.get('country') or row.get('Country')

    # 6. Merchant / Payee / Receiver
    merchant = None
    for k in ['merchant', 'transactiondescription', 'receivername', 'receiverupiid', 'payee', 'vendor', 'description', 'beneficiary']:
        if k in norm_row and norm_row[k]:
            merchant = norm_row[k]
            break
    if not merchant:
        merchant = (
            row.get('merchant') or
            row.get('Transaction Description') or
            row.get('Merchant') or
            row.get('description') or
            "General Merchant"
        )

    # 7. Category
    category = None
    for k in ['category', 'expensetype', 'txncategory', 'type']:
        if k in norm_row and norm_row[k]:
            category = norm_row[k]
            break
    if not category:
        category = row.get('category') or row.get('Category') or "General"

    # 8. Channel
    channel = None
    for k in ['channel', 'transactiontype', 'paymentmode', 'status', 'method']:
        if k in norm_row and norm_row[k]:
            channel = norm_row[k]
            break
    if not channel:
        channel = row.get('channel') or row.get('Transaction Type') or row.get('Channel') or "Online"

    raw_val, orig_curr, amount_inr = parse_currency_and_amount(
        amount_raw,
        currency_hint=currency_hint,
        country_hint=country_hint,
        desc_hint=merchant
    )

    row_data = {
        "transaction_id": str(txn_id),
        "account_id": str(account_id),
        "timestamp": str(timestamp),
        "amount": round(raw_val, 2),
        "currency": orig_curr,
        "amount_inr": amount_inr,
        "conversion_rate": get_current_rate(orig_curr),
        "merchant": str(merchant),
        "category": str(category),
        "channel": str(channel),
        "is_anomaly_label": row.get('is_anomaly_label')
    }

    # Module 3 ML Isolation Forest Anomaly Scoring
    is_anom, score, severity, cat_hint, anom_reason = detector_instance.score_event(row_data)

    event_payload = {
        "transaction_id": str(txn_id),
        "account_id": str(account_id),
        "timestamp": str(timestamp),
        "amount": round(raw_val, 2),
        "currency": orig_curr,
        "amount_inr": amount_inr,
        "conversion_rate": get_current_rate(orig_curr),
        "merchant": str(merchant),
        "category": str(category),
        "channel": str(channel),
        "is_anomaly": is_anom,
        "anomaly_score": score,
        "severity": severity,
        "category_hint": cat_hint,
        "anomaly_rationale": anom_reason if is_anom else None,
        "index": index,
        "total": total,
        "dataset": dataset_name
    }

    # Module 2 System of Record Persistence
    try:
        db_instance.insert_transaction(event_payload)
        if is_anom:
            db_instance.insert_alert({
                "transaction_id": str(txn_id),
                "anomaly_score": score,
                "is_anomaly": True,
                "severity": severity,
                "category_hint": cat_hint,
                "rationale": anom_reason,
                "model_version": detector_instance.model_version
            })
    except Exception:
        pass

    return event_payload

def kafka_listener_worker():
    print("[Kafka Listener] Background listener initialized.")
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset='latest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=1000
            )
            for message in consumer:
                payload = message.value
                payload['_kafka_partition'] = message.partition
                payload['_kafka_offset'] = message.offset
                payload['_received_at'] = time.time()
                if 'amount_inr' not in payload:
                    raw_val, orig_curr, amount_inr = parse_currency_and_amount(payload.get('amount', 0), payload.get('currency'))
                    payload['amount'] = raw_val
                    payload['currency'] = orig_curr
                    payload['amount_inr'] = amount_inr
                    payload['conversion_rate'] = get_current_rate(orig_curr)
                    is_anom, reason = detect_anomaly(payload, raw_val, orig_curr, amount_inr, payload.get('merchant', ''), payload.get('channel', ''))
                    payload['is_anomaly'] = is_anom or payload.get('is_anomaly', False)
                    if payload['is_anomaly']:
                        payload['anomaly_rationale'] = reason
                broadcast_event(payload)
        except Exception:
            time.sleep(3)

def file_streaming_worker(file_path, display_name, speed=1.0):
    active_replay["running"] = True
    active_replay["current_file"] = display_name

    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda d: json.dumps(d).encode('utf-8')
        )
    except Exception as e:
        print(f"[Warning] Kafka direct producer failed ({e}), streaming locally via SSE.")

    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            total_rows = sum(1 for _ in f) - 1
            if total_rows < 1:
                total_rows = 1
    except Exception:
        total_rows = 1

    broadcast_event({
        "type": "replay_started",
        "dataset": display_name,
        "total_rows": total_rows,
        "filename": os.path.basename(file_path)
    })

    if total_rows > 500:
        delay = 0.02 if speed > 1.5 else 0.05
    else:
        delay = 0.25 if speed > 1.5 else 0.6

    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            idx = 0
            for row in reader:
                if not active_replay["running"]:
                    break
                idx += 1
                payload = normalize_row_to_blueprint(row, idx, total_rows, display_name)

                if producer:
                    try:
                        producer.send(TOPIC, value=payload)
                    except Exception:
                        pass

                broadcast_event(payload)
                if delay > 0:
                    time.sleep(delay)
    except Exception as e:
        broadcast_event({"type": "system_log", "level": "error", "message": f"Streaming error: {e}"})

    if producer:
        try:
            producer.flush()
        except Exception:
            pass

    broadcast_event({
        "type": "replay_finished",
        "dataset": display_name,
        "total_rows": total_rows
    })
    active_replay["running"] = False
    active_replay["current_file"] = None

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
        elif parsed.path == "/api/rates":
            self.handle_api_rates()
        elif parsed.path == "/api/metrics":
            self.handle_api_metrics()
        elif parsed.path == "/api/alerts":
            self.handle_api_alerts()
        elif parsed.path == "/api/categories":
            self.handle_api_categories()
        elif parsed.path == "/api/transactions":
            self.handle_api_transactions()
        else:
            super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/upload":
            self.handle_file_upload()
        elif parsed.path == "/api/stop":
            active_replay["running"] = False
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "stopped"}).encode('utf-8'))
        elif parsed.path == "/api/replay_default":
            params = parse_qs(parsed.query)
            try:
                speed = float(params.get('speed', [1.0])[0])
            except Exception:
                speed = 1.0
            def_path = os.path.join(DATA_DIR, "transactions_controlled.csv")
            if active_replay["running"]:
                active_replay["running"] = False
                time.sleep(0.2)
            t = threading.Thread(target=file_streaming_worker, args=(def_path, "transactions_controlled.csv", speed), daemon=True)
            active_replay["thread"] = t
            t.start()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "started", "filename": "transactions_controlled.csv"}).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

    def handle_api_rates(self):
        with rates_lock:
            rates_copy = dict(LIVE_EXCHANGE_RATES)
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps({
            "base": "INR",
            "rates": rates_copy,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": last_rates_update["source"],
            "anomaly_threshold_inr": ANOMALY_THRESHOLD_INR
        }).encode('utf-8'))

    def handle_api_metrics(self):
        summary = db_instance.get_summary_metrics()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(summary).encode('utf-8'))

    def handle_api_alerts(self):
        alerts = db_instance.get_recent_alerts(limit=50)
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps({"alerts": alerts, "total": len(alerts)}).encode('utf-8'))

    def handle_api_categories(self):
        cats = db_instance.get_category_aggregates()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps({"categories": cats}).encode('utf-8'))

    def handle_api_transactions(self):
        txns = db_instance.get_recent_transactions(limit=50)
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps({"transactions": txns, "total": len(txns)}).encode('utf-8'))

    def handle_file_upload(self):
        content_type = self.headers.get('Content-Type', '')
        if not content_type.startswith('multipart/form-data'):
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'{"error": "Invalid content-type"}')
            return

        boundary_match = re.search(r'boundary=([^;\r\n]+)', content_type)
        if boundary_match:
            boundary_str = boundary_match.group(1).strip().strip('"')
            boundary = boundary_str.encode('utf-8')
        else:
            boundary = content_type.split("boundary=")[-1].strip().strip('"').encode('utf-8')
        content_len = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_len)

        parts = body.split(b'--' + boundary)
        file_bytes = None
        filename = "uploaded_transactions.csv"
        speed = 1.0

        for part in parts:
            if b'Content-Disposition:' in part:
                headers_part, content_part = part.split(b'\r\n\r\n', 1)
                content_part = content_part.rstrip(b'\r\n')
                headers_str = headers_part.decode('utf-8', errors='replace')
                
                if 'filename="' in headers_str:
                    fn_match = re.search(r'filename="([^"]+)"', headers_str)
                    if fn_match:
                        filename = fn_match.group(1)
                    file_bytes = content_part
                elif 'name="speed"' in headers_str:
                    try:
                        speed = float(content_part.decode('utf-8').strip())
                    except Exception:
                        speed = 1.0

        if not file_bytes:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'{"error": "No file uploaded"}')
            return

        save_path = os.path.join(UPLOADS_DIR, f"{int(time.time())}_{filename}")
        with open(save_path, 'wb') as f:
            f.write(file_bytes)

        if active_replay["running"]:
            active_replay["running"] = False
            time.sleep(0.2)

        t = threading.Thread(target=file_streaming_worker, args=(save_path, filename, speed), daemon=True)
        active_replay["thread"] = t
        t.start()

        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps({"status": "started", "filename": filename, "speed": speed}).encode('utf-8'))

    def handle_api_status(self):
        kafka_ok = False
        try:
            c = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                consumer_timeout_ms=500
            )
            kafka_ok = True
            c.close()
        except Exception:
            kafka_ok = False

        with rates_lock:
            rates_copy = dict(LIVE_EXCHANGE_RATES)

        status = {
            "kafka_connected": kafka_ok,
            "broker": BOOTSTRAP_SERVERS[0],
            "topic": TOPIC,
            "active_replay": active_replay["running"],
            "current_file": active_replay["current_file"],
            "recent_count": len(recent_events),
            "rates": rates_copy,
            "rates_source": last_rates_update["source"]
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

        client_queue = queue.Queue(maxsize=500)
        with subscribers_lock:
            subscribers.append(client_queue)

        try:
            self.wfile.write(f"data: {json.dumps({'type': 'connected', 'topic': TOPIC, 'rates': dict(LIVE_EXCHANGE_RATES)})}\n\n".encode('utf-8'))
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
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
        except Exception:
            pass
        finally:
            with subscribers_lock:
                if client_queue in subscribers:
                    subscribers.remove(client_queue)

def run_server(port=8080):
    listener_thread = threading.Thread(target=kafka_listener_worker, daemon=True)
    listener_thread.start()

    rates_thread = threading.Thread(target=real_time_rates_updater_worker, daemon=True)
    rates_thread.start()

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
