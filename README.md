# AI Fraud Project

## Overview
This repository implements a real‑time fraud detection pipeline composed of three micro‑services:
- **transaction_api** – FastAPI service that validates incoming transaction data and publishes it to Kafka.
- **ml_service** – FastAPI service that loads a pre‑trained RandomForest model and provides a `/predict` endpoint.
- **fraud_consumer** – Consumer that reads from Kafka, enriches data, stores it in Redis/PostgreSQL and calls the ML service.

The system is containerised with Docker Compose and includes Prometheus metrics and a Grafana dashboard.

## Prerequisites
- Docker & Docker Compose (>= 2.20)
- Python 3.14 (for local development and running the simulator)
- (Optional) virtual environment if you want to run the services locally without Docker.

## Quick start (Docker)
```bash
# Build and start all services in detached mode
docker compose up --build -d

# Run the traffic simulator to generate synthetic transactions
python simulation_client.py
```

The API endpoints are exposed on:
- Transaction API: `http://localhost:8000/transactions`
- ML Service: `http://localhost:8001/predict`
- Prometheus metrics: `http://localhost:8000/metrics`

## Testing
All unit tests run inside the Docker containers to avoid platform‑specific binary issues:
```bash
# Transaction API tests
docker compose run --rm transaction-api python -m pytest test_main.py

# ML Service tests
docker compose run --rm ml-service python -m pytest test_main.py

# Fraud Consumer tests
docker compose run --rm fraud-consumer python -m pytest test_main.py
```

## Configuration
Copy the example environment file and adjust values as needed:
```bash
cp .env.example .env
```
Then restart the stack:
```bash
docker compose down -v && docker compose up --build -d
```

## Documentation
- The API OpenAPI docs are available at `http://localhost:8000/docs`.
- Grafana dashboards (if enabled) can be accessed at `http://localhost:3000`.

## License
MIT – feel free to modify and extend.
