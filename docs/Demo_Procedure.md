# FinX: 25% Implementation Demonstration (Module 1)

This document provides the exact step-by-step procedure to demonstrate the Data Ingestion module to the panel.

## Architecture Context
As per the Blueprint (Section 5: Four Modules), **Module 1: Data Ingestion** is responsible for publishing validated transaction events to a Kafka topic and supporting replayable demo data. 
We have implemented this using Apache Kafka and a Python ingestion script that correctly formats the data into the agreed schema (transaction ID, account ID, timestamp, amount, merchant, category, channel).

---

## 1. Environment Setup

*Prerequisites: Ensure Docker Desktop is open and running on your machine.*

1. **Clone the repository:**
   ```bash
   git clone https://github.com/SrujaN1906/FinX.git
   cd FinX
   ```

2. **Start the Infrastructure (Kafka & Zookeeper):**
   ```bash
   docker-compose up -d
   ```
   *Note: This spins up the streaming backbone.*

3. **Install Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

---

## 2. Demonstrate Data Schema Alignment
*Action:* Open `data/transactions_huge.csv` in your IDE.
*Script:* "According to Section 5 of the Blueprint, Module 1 must ingest validated transaction events. Here is our official dataset. As you can see, the columns perfectly match the Blueprint's required schema."

*Action:* Open `data/transactions_controlled.csv` in your IDE.
*Script:* "Because the Blueprint requires a 'repeatable demo with seeded scenarios', we also created a 'Controlled Dataset'. This contains exactly 10 transactions, allowing us to cleanly trace a specific anomaly through the pipeline."

---

## 3. Run the Demonstration

### A. The Huge Dataset (Scale Demonstration)
*Action:* Run the following command in your terminal:
```bash
python streaming/dataset_replay.py --dataset huge
```
*Script:* "This is our Data Ingestion module actively publishing the large dataset into Apache Kafka. It transforms the CSV rows into JSON payloads and streams them rapidly to our `finx-transactions` topic."
*(Press `Ctrl+C` to stop the stream after a few seconds).*

### B. The Controlled Dataset (Scenario Demonstration)
*Action:* Run the following command in your terminal:
```bash
python streaming/dataset_replay.py --dataset controlled
```
*Script:* "To prove that our ingestion pipeline can handle specific anomaly scenarios precisely as the Blueprint demands, we will transition to our Controlled Data. It ingests these 10 transactions slowly. Right here, we have intentionally seeded an anomalous transaction. In the next modules, this is the exact transaction our Machine Learning model will catch."

---

## 4. Teardown
When the demo is over, gracefully shut down the Kafka cluster:
```bash
docker-compose down
```
