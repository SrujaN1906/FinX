# FinX

This repository contains the **Real-Time Financial Insight and Anomaly Detection System** prototype (**Module 1: Data Ingestion**).

### Status
- **25% Implementation completed (Module 1: Real-Time Data Ingestion & Demo System).**

---

## Architecture Overview

```
+-------------------+      +--------------------+      +------------------------+
|   Data Replay     | ---> |   Kafka Producer   | ---> | Apache Kafka Broker    |
| (CSV Engine / SSE)|      | (JSON Serializer)  |      | Topic: finx-transactions|
+-------------------+      +--------------------+      +-----------+------------+
                                                                   |
                                                                   v
+------------------------+                             +-----------+------------+
| FinX Live Demo Web UI  | <---------------------------| Kafka Consumer Monitor |
| (http://localhost:8080)|                             | (Schema & Anomaly Sink)|
+------------------------+                             +------------------------+
```

- **Streaming Backbone:** Apache Kafka 7.4.0 & Zookeeper (Docker Compose).
- **Data Schemas:** Validated financial transactions (`transaction_id`, `account_id`, `timestamp`, `amount`, `merchant`, `category`, `channel`).
- **Real-Time Visualization:** Interactive web dashboard with real-time throughput metrics, volume charts, anomaly banner, and JSON schema inspector.

---

## Quick Start Guide

### 1. Start Kafka & Zookeeper Infrastructure
Ensure Docker Desktop is running, then execute:
```bash
docker-compose up -d
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Launch the Interactive Web Dashboard
```bash
python backend/server.py
```
Open **[http://localhost:8080](http://localhost:8080)** in your browser:
- Click **"Replay Controlled Dataset (10 Txns)"** to see real-time streaming and the seeded anomaly detection.
- Click **"Stream Scale Dataset (10k Txns)"** to run high-throughput velocity testing.

---

## CLI Ingestion & Replay Tools

### Run the Real-Time Terminal Consumer Monitor
```bash
python streaming/consumer_monitor.py
```

### Run Controlled Dataset Replay (Scenario Demonstration)
Streams 10 transactions with a deliberate anomaly seeded at Transaction #8 ($11,496.98 at Unknown Overseas Merchant):
```bash
python streaming/dataset_replay.py --dataset controlled
```

### Run Huge Dataset Replay (Scale Demonstration)
Streams 10,000 transactions at rapid pace:
```bash
python streaming/dataset_replay.py --dataset huge
```

---

## Project Structure

```
FinX/
├── backend/
│   └── server.py                  # Multi-threaded HTTP & SSE live streaming server
├── data/
│   ├── transactions_controlled.csv # 10-row dataset with seeded anomaly
│   └── transactions_huge.csv       # 10,000-row scale dataset
├── docs/
│   ├── Demo_Procedure.md          # Step-by-step panel presentation guide
│   └── Demo_Procedure_Guide.pdf   # Downloadable PDF guide
├── frontend/
│   ├── index.html                 # Modern dashboard interface
│   ├── index.css                  # Fintech dark theme & glassmorphic styles
│   └── app.js                     # Real-time SSE subscriber & chart controller
├── streaming/
│   ├── dataset_replay.py          # Kafka producer engine
│   └── consumer_monitor.py        # Real-time Kafka consumer terminal monitor
├── docker-compose.yml             # Kafka & Zookeeper configuration
└── requirements.txt               # Python package dependencies
```
