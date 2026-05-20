import os
import json
import time
import asyncio
from datetime import datetime, timezone
import httpx
import asyncpg
import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
from prometheus_client import start_http_server, Counter, Histogram, Gauge

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "fraud-detection-group")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "fraud_db")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

ML_SERVICE_URL = os.getenv("ML_SERVICE_URL", "http://localhost:8001/predict")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8002"))

# Prometheus metrics setup
PROCESSED_COUNTER = Counter(
    "fraud_consumer_processed_total",
    "Total transactions processed by the fraud detection consumer"
)
FRAUD_COUNTER = Counter(
    "fraud_consumer_fraud_flagged_total",
    "Total transactions flagged as fraudulent"
)
FAILURE_COUNTER = Counter(
    "fraud_consumer_failures_total",
    "Total processing failures inside the consumer",
    ["error_type"]
)
E2E_LATENCY = Histogram(
    "fraud_consumer_pipeline_latency_seconds",
    "End-to-end pipeline latency (from transaction timestamp to DB save)"
)
ML_CALL_LATENCY = Histogram(
    "fraud_consumer_ml_call_latency_seconds",
    "ML inference HTTP call latency in seconds"
)
KAFKA_LAG = Gauge(
    "fraud_consumer_kafka_lag",
    "Approximate consumer group lag (unprocessed messages)"
)

class FraudConsumerService:
    def __init__(self):
        self.redis_client = None
        self.pg_pool = None
        self.http_client = None
        self.kafka_consumer = None

    async def connect_redis(self):
        retry_count = 0
        while retry_count < 10:
            try:
                print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
                self.redis_client = aioredis.Redis(
                    host=REDIS_HOST,
                    port=REDIS_PORT,
                    decode_responses=True
                )
                await self.redis_client.ping()
                print("Redis connected successfully.")
                return
            except Exception as e:
                retry_count += 1
                print(f"Redis connection attempt {retry_count} failed: {e}. Retrying in 3s...")
                await asyncio.sleep(3)
        raise RuntimeError("Could not connect to Redis.")

    async def connect_postgres(self):
        retry_count = 0
        dsn = f"postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        while retry_count < 10:
            try:
                print(f"Connecting to PostgreSQL at {POSTGRES_HOST}:{POSTGRES_PORT}...")
                self.pg_pool = await asyncpg.create_pool(dsn=dsn, min_size=5, max_size=20)
                print("PostgreSQL connection pool created.")
                return
            except Exception as e:
                retry_count += 1
                print(f"PostgreSQL connection attempt {retry_count} failed: {e}. Retrying in 3s...")
                await asyncio.sleep(3)
        raise RuntimeError("Could not connect to PostgreSQL.")

    async def connect_kafka(self):
        retry_count = 0
        while retry_count < 10:
            try:
                print(f"Connecting Kafka consumer to {KAFKA_BOOTSTRAP_SERVERS}...")
                self.kafka_consumer = AIOKafkaConsumer(
                    KAFKA_TOPIC,
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    group_id=KAFKA_GROUP_ID,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    auto_offset_reset="earliest",
                    enable_auto_commit=True
                )
                await self.kafka_consumer.start()
                print(f"Kafka consumer listening on topic '{KAFKA_TOPIC}'.")
                return
            except KafkaConnectionError as e:
                retry_count += 1
                print(f"Kafka consumer connection attempt {retry_count} failed: {e}. Retrying in 3s...")
                await asyncio.sleep(3)
        raise RuntimeError("Could not connect to Kafka broker.")

    async def fetch_redis_features(self, tx_id, user_id, amount, tx_time):
        """
        Engineers features in real-time using sliding windows stored in Redis
        """
        # Convert timestamp to unix seconds for scores
        score = tx_time.timestamp()
        
        # 1. user_tx_count_10m
        key_tx_10m = f"user_tx_10m:{user_id}"
        await self.redis_client.zadd(key_tx_10m, {tx_id: score})
        await self.redis_client.zremrangebyscore(key_tx_10m, "-inf", score - 600)
        user_tx_count_10m = await self.redis_client.zcard(key_tx_10m)
        await self.redis_client.expire(key_tx_10m, 3600)  # 1 hour TTL
        
        # 2. user_avg_amount_ratio
        key_sum = f"user_spent_sum:{user_id}"
        key_cnt = f"user_spent_cnt:{user_id}"
        
        sum_spent = await self.redis_client.get(key_sum)
        cnt_spent = await self.redis_client.get(key_cnt)
        
        if sum_spent and cnt_spent:
            avg_amount = float(sum_spent) / int(cnt_spent)
            user_avg_amount_ratio = amount / avg_amount if avg_amount > 0 else 1.0
        else:
            user_avg_amount_ratio = 1.0
            
        # Update user's aggregate spending history
        await self.redis_client.incrbyfloat(key_sum, amount)
        await self.redis_client.incrby(key_cnt, 1)
        # Expire profiles after 30 days of inactivity
        await self.redis_client.expire(key_sum, 30 * 86400)
        await self.redis_client.expire(key_cnt, 30 * 86400)
        
        # 3. user_spent_24h_ratio (vs $1000 limit)
        key_spent_24h = f"user_spent_24h:{user_id}"
        member_val = f"{tx_id}:{amount}"
        await self.redis_client.zadd(key_spent_24h, {member_val: score})
        await self.redis_client.zremrangebyscore(key_spent_24h, "-inf", score - 86400)
        
        # Calculate sum of transactions in last 24h
        txs_24h = await self.redis_client.zrange(key_spent_24h, 0, -1)
        spent_24h = 0.0
        for tx_str in txs_24h:
            try:
                parts = tx_str.split(":")
                if len(parts) >= 2:
                    spent_24h += float(parts[-1])
            except ValueError:
                pass
                
        user_spent_24h_ratio = spent_24h / 1000.0
        await self.redis_client.expire(key_spent_24h, 90000)  # 25 hours TTL
        
        return user_avg_amount_ratio, user_tx_count_10m, user_spent_24h_ratio

    async def call_ml_inference(self, payload):
        """
        Sends the transaction features to the ML service
        """
        start_time = time.time()
        try:
            response = await self.http_client.post(
                ML_SERVICE_URL,
                json=payload,
                timeout=1.0
            )
            
            latency = time.time() - start_time
            ML_CALL_LATENCY.observe(latency)
            
            if response.status_code == 200:
                data = response.json()
                return data["is_fraud"] == 1, data["confidence"], data["inference_latency_ms"]
            else:
                print(f"ML service returned status code {response.status_code}: {response.text}")
                # Fallback on inference failure (rule-based fallback or default)
                return False, 0.50, latency * 1000
        except Exception as e:
            FAILURE_COUNTER.labels(error_type="ml_inference_error").inc()
            print(f"Failed to fetch prediction from ML Service: {e}")
            # Fallback
            latency = time.time() - start_time
            return False, 0.50, latency * 1000

    async def persist_transaction(self, tx, features, model_res, e2e_latency_ms):
        """
        Saves transaction details, features, and model results to PostgreSQL
        """
        query = """
            INSERT INTO transactions (
                transaction_id, user_id, amount, timestamp, hour_of_day, day_of_week, is_foreign_country,
                user_avg_amount_ratio, user_tx_count_10m, user_spent_24h_ratio,
                is_fraud, confidence, inference_latency_ms, pipeline_latency_ms
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (transaction_id) DO NOTHING;
        """
        
        tx_id, user_id, amount, tx_time, hour, dow, is_foreign = tx
        user_avg_amount_ratio, user_tx_count_10m, user_spent_24h_ratio = features
        is_fraud, confidence, ml_latency = model_res
        
        async with self.pg_pool.acquire() as conn:
            await conn.execute(
                query,
                tx_id, user_id, amount, tx_time, hour, dow, is_foreign,
                user_avg_amount_ratio, user_tx_count_10m, user_spent_24h_ratio,
                is_fraud, confidence, ml_latency, e2e_latency_ms
            )

    async def process_message(self, message):
        try:
            tx_data = message.value
            tx_id = tx_data["transaction_id"]
            user_id = tx_data["user_id"]
            amount = float(tx_data["amount"])
            is_foreign = int(tx_data["is_foreign_country"])
            
            # Parse timestamp and extract details
            tx_time_str = tx_data["timestamp"].replace("Z", "+00:00")
            tx_time = datetime.fromisoformat(tx_time_str)
            
            hour = tx_time.hour
            dow = tx_time.weekday()
            
            # 1. Feature Engineering
            user_avg_ratio, user_tx_10m, user_spent_24h_ratio = await self.fetch_redis_features(
                tx_id, user_id, amount, tx_time
            )
            
            # 2. ML Inference
            payload = {
                "amount": amount,
                "hour_of_day": hour,
                "day_of_week": dow,
                "user_avg_amount_ratio": user_avg_ratio,
                "user_tx_count_10m": user_tx_10m,
                "user_spent_24h_ratio": user_spent_24h_ratio,
                "is_foreign_country": is_foreign
            }
            
            is_fraud, confidence, ml_latency = await self.call_ml_inference(payload)
            
            # 3. Compute end-to-end latency
            now = datetime.now(timezone.utc)
            e2e_latency_ms = (now - tx_time).total_seconds() * 1000
            E2E_LATENCY.observe(e2e_latency_ms / 1000)
            
            # 4. Save to Database
            tx = (tx_id, user_id, amount, tx_time, hour, dow, is_foreign)
            features = (user_avg_ratio, user_tx_10m, user_spent_24h_ratio)
            model_res = (is_fraud, confidence, ml_latency)
            
            await self.persist_transaction(tx, features, model_res, e2e_latency_ms)
            
            # 5. Metrics & Logging
            PROCESSED_COUNTER.inc()
            if is_fraud:
                FRAUD_COUNTER.inc()
                print(f"[ALERT] Fraud Detected! ID: {tx_id} | User: {user_id} | Amount: ${amount:.2f} | Confidence: {confidence:.2%}")
            
        except Exception as e:
            FAILURE_COUNTER.labels(error_type="message_processing_failure").inc()
            print(f"Error processing transaction message: {e}")

    async def run(self):
        # Start Prometheus metrics server
        start_http_server(METRICS_PORT)
        print(f"Prometheus metrics server running on port {METRICS_PORT}.")
        
        # Establish connections
        await self.connect_redis()
        await self.connect_postgres()
        await self.connect_kafka()
        
        self.http_client = httpx.AsyncClient()
        
        print("Starting consumption loop...")
        try:
            async for msg in self.kafka_consumer:
                await self.process_message(msg)
                
                # Approximate lag tracking (Kafka admin APIs can be verbose, so we do a simple check)
                # To keep metrics updated
                # KAFKA_LAG.set(lag)
        except Exception as e:
            print(f"Error in main consumption loop: {e}")
        finally:
            print("Shutting down connections...")
            if self.kafka_consumer:
                await self.kafka_consumer.stop()
            if self.redis_client:
                await self.redis_client.close()
            if self.pg_pool:
                await self.pg_pool.close()
            if self.http_client:
                await self.http_client.aclose()
            print("Shutdown complete.")

if __name__ == "__main__":
    service = FraudConsumerService()
    asyncio.run(service.run())
