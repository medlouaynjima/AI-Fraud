from unittest.mock import MagicMock
import pytest
from fastapi.testclient import TestClient
import sys
import os

# Add the ml_service directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import main

# Mock model and scaler before endpoints are invoked
main.model = MagicMock()
main.scaler = MagicMock()

# Mock scaler.transform to return a dummy array
main.scaler.transform.return_value = [[0.0] * 7]
# Mock model.predict and predict_proba
main.model.predict.return_value = [0]
main.model.predict_proba.return_value = [[0.95, 0.05]]

client = TestClient(main.app)

def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy", "model_loaded": True}

def test_predict_legit():
    payload = {
        "amount": 50.0,
        "hour_of_day": 12,
        "day_of_week": 3,
        "user_avg_amount_ratio": 1.0,
        "user_tx_count_10m": 1,
        "user_spent_24h_ratio": 0.2,
        "is_foreign_country": 0
    }
    # Temporarily override model predictions for legit case
    main.model.predict.return_value = [0]
    main.model.predict_proba.return_value = [[0.95, 0.05]]
    
    response = client.post("/predict", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["is_fraud"] == 0
    assert data["confidence"] == 0.95
    assert "inference_latency_ms" in data

def test_predict_fraud():
    payload = {
        "amount": 999.99,
        "hour_of_day": 2,
        "day_of_week": 1,
        "user_avg_amount_ratio": 15.0,
        "user_tx_count_10m": 10,
        "user_spent_24h_ratio": 2.5,
        "is_foreign_country": 1
    }
    # Temporarily override model predictions for fraud case
    main.model.predict.return_value = [1]
    main.model.predict_proba.return_value = [[0.05, 0.95]]
    
    response = client.post("/predict", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["is_fraud"] == 1
    assert data["confidence"] == 0.95
    assert "inference_latency_ms" in data
