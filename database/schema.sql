-- FinX Database Schema (PostgreSQL)
-- Module 2 & 3 persistence: Processed transactions, anomaly scores, alerts, and aggregates

-- 1. Processed Transactions Table
CREATE TABLE IF NOT EXISTS processed_transactions (
    transaction_id VARCHAR(100) PRIMARY KEY,
    account_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    original_amount NUMERIC(15, 2) NOT NULL,
    original_currency VARCHAR(10) NOT NULL DEFAULT 'INR',
    amount_inr NUMERIC(15, 2) NOT NULL,
    conversion_rate NUMERIC(10, 4) NOT NULL DEFAULT 1.0,
    merchant VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    channel VARCHAR(50) NOT NULL,
    location VARCHAR(100),
    processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Index for speedy queries
CREATE INDEX IF NOT EXISTS idx_txn_account ON processed_transactions(account_id);
CREATE INDEX IF NOT EXISTS idx_txn_timestamp ON processed_transactions(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_txn_category ON processed_transactions(category);

-- 2. Anomaly Alerts Table
CREATE TABLE IF NOT EXISTS anomaly_alerts (
    alert_id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(100) REFERENCES processed_transactions(transaction_id) ON DELETE CASCADE,
    anomaly_score NUMERIC(6, 4) NOT NULL, -- Isolation Forest decision score (normalized 0 to 1)
    is_anomaly BOOLEAN NOT NULL DEFAULT TRUE,
    severity VARCHAR(20) NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    category_hint VARCHAR(100) NOT NULL, -- e.g., 'Amount deviation', 'Velocity outlier', 'Suspicious overseas'
    rationale TEXT NOT NULL,
    model_version VARCHAR(50) NOT NULL DEFAULT 'isolation_forest_v1.0',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_alert_txn ON anomaly_alerts(transaction_id);
CREATE INDEX IF NOT EXISTS idx_alert_severity ON anomaly_alerts(severity);
CREATE INDEX IF NOT EXISTS idx_alert_created ON anomaly_alerts(created_at DESC);

-- 3. Rolling Analytics & Aggregates Table
CREATE TABLE IF NOT EXISTS stream_aggregates (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    total_transactions INT NOT NULL,
    total_amount_inr NUMERIC(18, 2) NOT NULL,
    anomaly_count INT NOT NULL,
    category_breakdown JSONB,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_agg_window ON stream_aggregates(window_start DESC);
