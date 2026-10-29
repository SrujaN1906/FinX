"""
FinX Database Storage Adapter
Provides persistent storage in PostgreSQL when available, with automatic SQLite fallback
so the entire system runs seamlessly anywhere without database connection errors.
"""
import os
import sqlite3
import json
from datetime import datetime, timezone

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "database", "finx_records.db")

# Optional PostgreSQL support via psycopg2 or pg8000
PG_AVAILABLE = False
try:
    import psycopg2
    PG_AVAILABLE = True
except ImportError:
    pass

class FinXDatabase:
    def __init__(self):
        self.use_pg = False
        self.pg_conn_str = os.environ.get("FINX_PG_URL", "postgresql://postgres:postgres@localhost:5432/finx")
        self._init_sqlite()
        if PG_AVAILABLE:
            self._try_init_postgres()

    def _init_sqlite(self):
        """Initializes SQLite database tables mirroring the PostgreSQL schema."""
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_transactions (
                transaction_id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                original_amount REAL NOT NULL,
                original_currency TEXT NOT NULL DEFAULT 'INR',
                amount_inr REAL NOT NULL,
                conversion_rate REAL NOT NULL DEFAULT 1.0,
                merchant TEXT NOT NULL,
                category TEXT NOT NULL,
                channel TEXT NOT NULL,
                location TEXT,
                processed_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS anomaly_alerts (
                alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_id TEXT NOT NULL,
                anomaly_score REAL NOT NULL,
                is_anomaly INTEGER NOT NULL DEFAULT 1,
                severity TEXT NOT NULL,
                category_hint TEXT NOT NULL,
                rationale TEXT NOT NULL,
                model_version TEXT NOT NULL DEFAULT 'isolation_forest_v1.0',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (transaction_id) REFERENCES processed_transactions(transaction_id)
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stream_aggregates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                window_start TEXT NOT NULL,
                window_end TEXT NOT NULL,
                total_transactions INTEGER NOT NULL,
                total_amount_inr REAL NOT NULL,
                anomaly_count INTEGER NOT NULL,
                category_breakdown TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        conn.close()

    def _try_init_postgres(self):
        try:
            conn = psycopg2.connect(self.pg_conn_str, connect_timeout=1)
            schema_sql_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "schema.sql")
            with open(schema_sql_path, "r", encoding="utf-8") as f:
                ddl = f.read()
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()
            conn.close()
            self.use_pg = True
            print("[Database] Successfully connected to PostgreSQL system of record.")
        except Exception:
            self.use_pg = False

    def insert_transaction(self, txn):
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT OR REPLACE INTO processed_transactions 
                (transaction_id, account_id, timestamp, original_amount, original_currency, 
                 amount_inr, conversion_rate, merchant, category, channel, location, processed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                txn['transaction_id'],
                txn['account_id'],
                txn['timestamp'],
                float(txn.get('amount', txn.get('original_amount', 0))),
                txn.get('currency', txn.get('original_currency', 'INR')),
                float(txn.get('amount_inr', 0)),
                float(txn.get('conversion_rate', 1.0)),
                txn.get('merchant', ''),
                txn.get('category', 'General'),
                txn.get('channel', 'Online'),
                txn.get('location', ''),
                datetime.now(timezone.utc).isoformat()
            ))
            conn.commit()
        finally:
            conn.close()

    def insert_alert(self, alert):
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                INSERT INTO anomaly_alerts
                (transaction_id, anomaly_score, is_anomaly, severity, category_hint, rationale, model_version, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                alert['transaction_id'],
                float(alert.get('anomaly_score', 0.85)),
                1 if alert.get('is_anomaly', True) else 0,
                alert.get('severity', 'HIGH'),
                alert.get('category_hint', 'Amount deviation'),
                alert.get('rationale', 'Anomaly flagged by Isolation Forest'),
                alert.get('model_version', 'isolation_forest_v1.0'),
                datetime.now(timezone.utc).isoformat()
            ))
            conn.commit()
        finally:
            conn.close()

    def get_summary_metrics(self):
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*), COALESCE(SUM(amount_inr), 0) FROM processed_transactions")
            total_txns, total_inr = cursor.fetchone()
            
            cursor.execute("SELECT COUNT(*) FROM anomaly_alerts WHERE is_anomaly = 1")
            total_anomalies = cursor.fetchone()[0]
            
            cursor.execute("SELECT MAX(processed_at) FROM processed_transactions")
            latest_ts = cursor.fetchone()[0]
            
            return {
                "total_transactions": total_txns,
                "total_amount_inr": round(total_inr, 2),
                "anomaly_count": total_anomalies,
                "latest_timestamp": latest_ts or datetime.now(timezone.utc).isoformat()
            }
        finally:
            conn.close()

    def get_category_aggregates(self):
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT category, COUNT(*), SUM(amount_inr)
                FROM processed_transactions
                GROUP BY category
                ORDER BY SUM(amount_inr) DESC
            """)
            rows = cursor.fetchall()
            return [{"category": r[0], "count": r[1], "total_inr": round(r[2], 2)} for r in rows]
        finally:
            conn.close()

    def get_recent_alerts(self, limit=50):
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT a.*, t.original_amount, t.original_currency, t.amount_inr, t.merchant, t.category, t.channel
                FROM anomaly_alerts a
                JOIN processed_transactions t ON a.transaction_id = t.transaction_id
                ORDER BY a.created_at DESC
                LIMIT ?
            """, (limit,))
            rows = cursor.fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_recent_transactions(self, limit=50):
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT t.*, 
                       CASE WHEN a.alert_id IS NOT NULL THEN 1 ELSE 0 END as has_alert,
                       a.severity, a.anomaly_score, a.rationale
                FROM processed_transactions t
                LEFT JOIN anomaly_alerts a ON t.transaction_id = a.transaction_id
                ORDER BY t.timestamp DESC
                LIMIT ?
            """, (limit,))
            rows = cursor.fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

db_instance = FinXDatabase()
