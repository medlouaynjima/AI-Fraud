import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime, timezone
import sys
import os

# Add the fraud_consumer directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from main import FraudConsumerService

@pytest.mark.asyncio
async def test_fetch_redis_features():
    service = FraudConsumerService()
    service.redis_client = MagicMock()
    
    # Mock redis calls
    service.redis_client.zadd = AsyncMock()
    service.redis_client.zremrangebyscore = AsyncMock()
    service.redis_client.zcard = AsyncMock(return_value=3)
    service.redis_client.expire = AsyncMock()
    
    # Mock get(key_sum) and get(key_cnt)
    async def mock_get(key):
        if "sum" in key:
            return "100.0"
        if "cnt" in key:
            return "2"
        return None
    service.redis_client.get = AsyncMock(side_effect=mock_get)
    
    service.redis_client.incrbyfloat = AsyncMock()
    service.redis_client.incrby = AsyncMock()
    service.redis_client.zrange = AsyncMock(return_value=["tx1:50.0", "tx2:20.0"])
    
    tx_time = datetime(2026, 5, 20, 20, 0, 0, tzinfo=timezone.utc)
    user_avg_ratio, user_tx_10m, user_spent_24h_ratio = await service.fetch_redis_features(
        "tx_test", "usr_test", 50.0, tx_time
    )
    
    # Average spent is 100 / 2 = 50.0. User amount is 50.0. Ratio should be 50 / 50 = 1.0.
    assert user_avg_ratio == 1.0
    # user_tx_count_10m mocked as 3
    assert user_tx_10m == 3
    # 24h spent sum mocked as 50 + 20 = 70.0. Ratio is 70.0 / 1000.0 = 0.07.
    assert user_spent_24h_ratio == 0.07

@pytest.mark.asyncio
async def test_call_ml_inference_success():
    service = FraudConsumerService()
    service.http_client = MagicMock()
    
    # Mock http response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json = MagicMock(return_value={
        "is_fraud": 1,
        "confidence": 0.88,
        "inference_latency_ms": 12.5
    })
    
    service.http_client.post = AsyncMock(return_value=mock_response)
    
    payload = {
        "amount": 100.0,
        "hour_of_day": 14,
        "day_of_week": 2,
        "user_avg_amount_ratio": 1.2,
        "user_tx_count_10m": 1,
        "user_spent_24h_ratio": 0.1,
        "is_foreign_country": 0
    }
    
    is_fraud, confidence, latency = await service.call_ml_inference(payload)
    
    assert is_fraud is True
    assert confidence == 0.88
    assert latency == 12.5

@pytest.mark.asyncio
async def test_call_ml_inference_failure_fallback():
    service = FraudConsumerService()
    service.http_client = MagicMock()
    
    # Mock http response with an error
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"
    
    service.http_client.post = AsyncMock(return_value=mock_response)
    
    payload = {
        "amount": 100.0,
        "hour_of_day": 14,
        "day_of_week": 2,
        "user_avg_amount_ratio": 1.2,
        "user_tx_count_10m": 1,
        "user_spent_24h_ratio": 0.1,
        "is_foreign_country": 0
    }
    
    is_fraud, confidence, latency = await service.call_ml_inference(payload)
    
    # Should fallback to False, 0.50 confidence
    assert is_fraud is False
    assert confidence == 0.50

@pytest.mark.asyncio
async def test_process_message():
    service = FraudConsumerService()
    
    # Mock inner methods
    service.fetch_redis_features = AsyncMock(return_value=(1.0, 3, 0.07))
    service.call_ml_inference = AsyncMock(return_value=(True, 0.92, 15.0))
    service.persist_transaction = AsyncMock()
    
    # Mock Kafka message
    mock_message = MagicMock()
    mock_message.value = {
        "transaction_id": "tx_abc123",
        "user_id": "usr_999",
        "amount": 150.00,
        "timestamp": "2026-05-20T20:00:00Z",
        "is_foreign_country": 1
    }
    
    await service.process_message(mock_message)
    
    # Ensure inner pipeline methods were called with correct engineering inputs
    service.fetch_redis_features.assert_called_once()
    service.call_ml_inference.assert_called_once_with({
        "amount": 150.00,
        "hour_of_day": 20,
        "day_of_week": 2,  # 2026-05-20 is Wednesday (2)
        "user_avg_amount_ratio": 1.0,
        "user_tx_count_10m": 3,
        "user_spent_24h_ratio": 0.07,
        "is_foreign_country": 1
    })
    service.persist_transaction.assert_called_once()
