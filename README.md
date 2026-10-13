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
- **Data Schemas:** Validated financial transactions with automated schema adaptation for custom CSVs.
- **Multi-Currency Normalizer:** Ingests international transactions (USD, EUR, GBP, AED, SGD, CAD, JPY, etc.) and calculates real-time normalized values in **₹ INR**.
- **Dual-Currency Display:** Live stream cards and inspector present both original foreign currency amount and converted ₹ INR value.
- **Fail-Safe Anomaly Detection:** Outliers and suspicious international merchants are caught in normalized INR values (threshold ₹50,000+) without any currency bypass.
- **Non-Blocking Lag-Free Ingestion:** RequestAnimationFrame event batching ensures smooth 60fps streaming even with large datasets containing innumerable rows.

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
- **Upload File for Detection:** Upload any financial CSV file to stream and inspect transactions live.
- **Sample Data:** Test with pre-configured repeatable controlled data.
- **Pace Selection:** Choose between `i. Realtime` or `ii. 2x Speed`.
- **Anomaly Detection:** Any anomaly (seeded, high-value in INR, or suspicious international merchant) triggers the visual alert banner.

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
