-- Initialize database schema for Real-Time AI Fraud Monitoring

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    hour_of_day INT NOT NULL,
    day_of_week INT NOT NULL,
    is_foreign_country INT NOT NULL,
    
    -- Engineered Features (computed in consumer from Redis)
    user_avg_amount_ratio NUMERIC(12, 4) NOT NULL,
    user_tx_count_10m INT NOT NULL,
    user_spent_24h_ratio NUMERIC(12, 4) NOT NULL,
    
    -- ML Inference Results
    is_fraud BOOLEAN NOT NULL,
    confidence NUMERIC(6, 5) NOT NULL,
    inference_latency_ms NUMERIC(10, 2) NOT NULL,
    
    -- Performance Metrics
    pipeline_latency_ms NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Index for analytics and dashboards
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_is_fraud ON transactions (is_fraud);
CREATE INDEX IF NOT EXISTS idx_transactions_user_id ON transactions (user_id);
