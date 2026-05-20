import os
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, roc_auc_score

def generate_synthetic_data(n_samples=10000):
    np.random.seed(42)
    
    # Generate baseline features
    amount = np.random.exponential(scale=50, size=n_samples) + 1  # skewed amount
    hour_of_day = np.random.randint(0, 24, size=n_samples)
    day_of_week = np.random.randint(0, 7, size=n_samples)
    
    # User profile ratios
    user_avg_amount_ratio = np.random.lognormal(mean=0, sigma=0.5, size=n_samples)
    user_tx_count_10m = np.random.poisson(lam=1, size=n_samples)
    user_spent_24h_ratio = np.random.beta(a=2, b=5, size=n_samples) * 1.5
    is_foreign_country = np.random.choice([0, 1], size=n_samples, p=[0.92, 0.08])
    
    df = pd.DataFrame({
        'amount': amount,
        'hour_of_day': hour_of_day,
        'day_of_week': day_of_week,
        'user_avg_amount_ratio': user_avg_amount_ratio,
        'user_tx_count_10m': user_tx_count_10m,
        'user_spent_24h_ratio': user_spent_24h_ratio,
        'is_foreign_country': is_foreign_country
    })
    
    # Define logical rules for fraud
    # Rule 1: High amount, foreign, and late night
    cond1 = (df['amount'] > 200) & (df['is_foreign_country'] == 1) & ((df['hour_of_day'] < 6) | (df['hour_of_day'] > 22))
    
    # Rule 2: High frequency (velocity attack)
    cond2 = df['user_tx_count_10m'] > 4
    
    # Rule 3: Extreme transaction size relative to user history
    cond3 = (df['user_avg_amount_ratio'] > 7.0) & (df['amount'] > 150)
    
    # Rule 4: Exceeded daily limit ratio
    cond4 = df['user_spent_24h_ratio'] > 1.2
    
    # Combine fraud conditions with some noise (90% probability of fraud under rules, 1% baseline fraud)
    fraud_prob = np.zeros(n_samples)
    fraud_prob[cond1 | cond2 | cond3 | cond4] = 0.90
    
    # Random baseline fraud
    baseline_noise = np.random.choice([0, 1], size=n_samples, p=[0.99, 0.01])
    is_fraud = np.where(np.random.random(n_samples) < fraud_prob, 1, baseline_noise)
    
    df['is_fraud'] = is_fraud
    
    print(f"Generated {n_samples} records.")
    print(f"Fraud count: {df['is_fraud'].sum()} ({df['is_fraud'].mean() * 100:.2f}%)")
    
    return df

def train_model():
    print("Generating synthetic dataset...")
    df = generate_synthetic_data(15000)
    
    X = df.drop(columns=['is_fraud'])
    y = df['is_fraud']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    print("Scaling features...")
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    print("Training RandomForest model...")
    model = RandomForestClassifier(n_estimators=100, max_depth=8, random_state=42, class_weight='balanced')
    model.fit(X_train_scaled, y_train)
    
    y_pred = model.predict(X_test_scaled)
    y_prob = model.predict_proba(X_test_scaled)[:, 1]
    
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred))
    print(f"ROC AUC Score: {roc_auc_score(y_test, y_prob):.4f}")
    
    # Create ml_service directory if it doesn't exist
    os.makedirs(os.path.dirname(__file__), exist_ok=True)
    
    # Save artifacts
    model_path = os.path.join(os.path.dirname(__file__), 'model.joblib')
    scaler_path = os.path.join(os.path.dirname(__file__), 'scaler.joblib')
    
    joblib.dump(model, model_path)
    joblib.dump(scaler, scaler_path)
    print(f"Model saved to {model_path}")
    print(f"Scaler saved to {scaler_path}")

if __name__ == '__main__':
    train_model()
