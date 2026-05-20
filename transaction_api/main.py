import os
import json
import time
import asyncio
from datetime import datetime
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from prometheus_client import make_asgi_app, Counter, Histogram

app = FastAPI(title="Real-Time Transaction Ingestion API", version="1.0.0")

# Setup Prometheus metrics
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# Prometheus Counters and Histograms
INGESTED_COUNTER = Counter(
    "transaction_api_ingested_total", 
    "Total number of transactions ingested"
)
FAILED_COUNTER = Counter(
    "transaction_api_failed_total", 
    "Total number of failed ingestions"
)
INGEST_LATENCY = Histogram(
    "transaction_api_ingest_latency_seconds", 
    "Latency of transaction ingestion in seconds"
)

# Kafka configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")

producer = None

class Transaction(BaseModel):
    transaction_id: str = Field(..., description="Unique identifier for the transaction")
    user_id: str = Field(..., description="Unique identifier for the user")
    amount: float = Field(..., gt=0, description="Transaction amount (must be positive)")
    timestamp: str = Field(..., description="ISO 8601 transaction timestamp")
    is_foreign_country: int = Field(..., ge=0, le=1, description="1 if transaction is foreign, 0 otherwise")

    model_config = {
        "json_schema_extra": {
            "example": {
                "transaction_id": "tx_abc123xyz",
                "user_id": "usr_987654",
                "amount": 49.99,
                "timestamp": "2026-05-20T14:00:00Z",
                "is_foreign_country": 0
            }
        }
    }

async def initialize_producer():
    global producer
    retry_count = 0
    max_retries = 10
    delay = 3
    
    print(f"Connecting to Kafka broker at: {KAFKA_BOOTSTRAP_SERVERS}")
    while retry_count < max_retries:
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            await producer.start()
            print("Kafka Producer successfully started and connected.")
            return
        except KafkaConnectionError as e:
            retry_count += 1
            print(f"Kafka connection attempt {retry_count}/{max_retries} failed: {e}. Retrying in {delay} seconds...")
            await asyncio.sleep(delay)
    
    print("Could not connect to Kafka. Ingestion API will run, but publishing will fail.")

@app.on_event("startup")
async def startup_event():
    # Start Kafka Producer in the background so API boot is not blocked if Kafka is still starting
    asyncio.create_task(initialize_producer())

@app.on_event("shutdown")
async def shutdown_event():
    global producer
    if producer:
        print("Stopping Kafka Producer...")
        await producer.stop()
        print("Kafka Producer stopped.")

@app.get("/health")
async def health_check():
    kafka_status = "connected" if producer and producer.client.is_ready() else "disconnected"
    return {
        "status": "healthy",
        "kafka_status": kafka_status
    }

@app.post("/transactions", status_code=status.HTTP_202_ACCEPTED)
async def ingest_transaction(transaction: Transaction):
    global producer
    
    if not producer or not producer.client.is_ready():
        FAILED_COUNTER.inc()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Kafka broker is not available. Please try again later."
        )

    start_time = time.time()
    try:
        # Validate timestamp format
        try:
            datetime.fromisoformat(transaction.timestamp.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid timestamp format. Must be ISO 8601 (YYYY-MM-DDTHH:MM:SSZ)"
            )

        payload = transaction.model_dump()
        
        # Send asynchronously to Kafka topic
        await producer.send_and_wait(KAFKA_TOPIC, payload)
        
        # Record metrics
        latency = time.time() - start_time
        INGEST_LATENCY.observe(latency)
        INGESTED_COUNTER.inc()
        
        return {
            "status": "accepted",
            "transaction_id": transaction.transaction_id,
            "latency_ms": latency * 1000
        }
    except Exception as e:
        FAILED_COUNTER.inc()
        print(f"Error publishing transaction to Kafka: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to publish transaction: {str(e)}"
        )
