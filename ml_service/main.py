import os
import time
import joblib
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from prometheus_client import make_asgi_app, Counter, Histogram

app = FastAPI(title="Fraud Detection ML Inference Service", version="1.0.0")

# Setup Prometheus metrics
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

PREDICTIONS_COUNTER = Counter(
    "ml_service_predictions_total",
    "Total number of fraud predictions made",
    ["result"]
)
INFERENCE_LATENCY = Histogram(
    "ml_service_inference_latency_seconds",
    "Inference latency in seconds"
)

# Global variables for model and scaler
model = None
scaler = None

class TransactionFeatures(BaseModel):
    amount: float = Field(..., description="Transaction amount")
    hour_of_day: int = Field(..., ge=0, le=23, description="Hour of the day (0-23)")
    day_of_week: int = Field(..., ge=0, le=6, description="Day of the week (0-6)")
    user_avg_amount_ratio: float = Field(..., description="Amount / average user transaction amount")
    user_tx_count_10m: int = Field(..., ge=0, description="Transaction count in the last 10 minutes")
    user_spent_24h_ratio: float = Field(..., description="Cumulative spent in last 24h / daily limit")
    is_foreign_country: int = Field(..., ge=0, le=1, description="1 if transaction is foreign, 0 otherwise")

    model_config = {
        "json_schema_extra": {
            "example": {
                "amount": 250.50,
                "hour_of_day": 3,
                "day_of_week": 2,
                "user_avg_amount_ratio": 8.5,
                "user_tx_count_10m": 5,
                "user_spent_24h_ratio": 1.3,
                "is_foreign_country": 1
            }
        }
    }

@app.on_event("startup")
def load_model_artifacts():
    global model, scaler
    base_dir = os.path.dirname(__file__)
    model_path = os.path.join(base_dir, "model.joblib")
    scaler_path = os.path.join(base_dir, "scaler.joblib")

    if not os.path.exists(model_path) or not os.path.exists(scaler_path):
        # In a real environment, we'd raise an error, but we'll try to train the model on the fly
        # if artifacts are missing so the service doesn't crash on boot in Docker.
        print("Model artifacts not found! Training model on startup...")
        try:
            from train import train_model
            train_model()
        except Exception as e:
            print(f"Failed to auto-train model: {e}")
            raise RuntimeError("Model artifacts missing and training failed.")

    model = joblib.load(model_path)
    scaler = joblib.load(scaler_path)
    print("Model and scaler successfully loaded.")

@app.get("/health")
def health_check():
    if model is None or scaler is None:
        raise HTTPException(status_code=503, detail="Model not loaded")
    return {"status": "healthy", "model_loaded": True}

@app.post("/predict")
def predict_fraud(features: TransactionFeatures):
    if model is None or scaler is None:
        raise HTTPException(status_code=503, detail="Model artifacts are not loaded.")

    start_time = time.time()
    
    try:
        # Convert features to DataFrame with proper columns (order matters for the scaler/model)
        feature_dict = features.model_dump()
        df = pd.DataFrame([feature_dict])
        
        # Scale features
        scaled_features = scaler.transform(df)
        
        # Predict
        prediction = int(model.predict(scaled_features)[0])
        probabilities = model.predict_proba(scaled_features)[0]
        confidence = float(probabilities[prediction])
        
        # Metrics
        latency = time.time() - start_time
        INFERENCE_LATENCY.observe(latency)
        
        result_label = "fraud" if prediction == 1 else "legit"
        PREDICTIONS_COUNTER.labels(result=result_label).inc()
        
        return {
            "is_fraud": prediction,
            "confidence": confidence,
            "inference_latency_ms": latency * 1000
        }
    except Exception as e:
        print(f"Error during prediction: {e}")
        raise HTTPException(status_code=500, detail=f"Inference error: {str(e)}")
