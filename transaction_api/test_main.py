from unittest.mock import MagicMock, AsyncMock
import pytest
from fastapi.testclient import TestClient
import sys
import os

# Add the transaction_api directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import main

# Setup mock objects
main.producer = MagicMock()
main.producer.send_and_wait = AsyncMock()
main.producer_ready = True

client = TestClient(main.app)

def test_health_connected():
    main.producer_ready = True
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy", "kafka_status": "connected"}

def test_health_disconnected():
    main.producer_ready = False
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy", "kafka_status": "disconnected"}

def test_ingest_transaction_success():
    main.producer_ready = True
    main.producer.send_and_wait.reset_mock()
    
    payload = {
        "transaction_id": "test_tx_123",
        "user_id": "usr_test",
        "amount": 99.99,
        "timestamp": "2026-05-20T20:00:00Z",
        "is_foreign_country": 0
    }
    response = client.post("/transactions", json=payload)
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "accepted"
    assert data["transaction_id"] == "test_tx_123"
    main.producer.send_and_wait.assert_called_once_with(main.KAFKA_TOPIC, payload)

def test_ingest_transaction_invalid_timestamp():
    main.producer_ready = True
    
    payload = {
        "transaction_id": "test_tx_123",
        "user_id": "usr_test",
        "amount": 99.99,
        "timestamp": "invalid-timestamp",
        "is_foreign_country": 0
    }
    response = client.post("/transactions", json=payload)
    assert response.status_code == 400
    assert "Invalid timestamp format" in response.json()["detail"]

def test_ingest_transaction_negative_amount():
    main.producer_ready = True
    
    payload = {
        "transaction_id": "test_tx_123",
        "user_id": "usr_test",
        "amount": -10.00,
        "timestamp": "2026-05-20T20:00:00Z",
        "is_foreign_country": 0
    }
    response = client.post("/transactions", json=payload)
    assert response.status_code == 422  # Pydantic validation error

def test_ingest_transaction_kafka_unavailable():
    main.producer_ready = False
    
    payload = {
        "transaction_id": "test_tx_123",
        "user_id": "usr_test",
        "amount": 99.99,
        "timestamp": "2026-05-20T20:00:00Z",
        "is_foreign_country": 0
    }
    response = client.post("/transactions", json=payload)
    assert response.status_code == 503
    assert "Kafka broker is not available" in response.json()["detail"]
