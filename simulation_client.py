import time
import random
import json
import uuid
import urllib.request
import urllib.error
from datetime import datetime, timezone

# Target API URL
API_URL = "http://localhost:8000/transactions"

# Simulated list of users
USER_IDS = [f"usr_{100000 + i}" for i in range(100)]
USER_AVG_SPENT = {user_id: random.uniform(15, 60) for user_id in USER_IDS}
USER_TX_HISTORY = {user_id: [] for user_id in USER_IDS}

def send_transaction(payload):
    req = urllib.request.Request(
        API_URL, 
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    
    try:
        with urllib.request.urlopen(req) as res:
            res_data = json.loads(res.read().decode("utf-8"))
            return res_data, None
    except urllib.error.HTTPError as e:
        try:
            err_msg = json.loads(e.read().decode("utf-8"))
        except Exception:
            err_msg = e.reason
        return None, f"HTTP Error {e.code}: {err_msg}"
    except urllib.error.URLError as e:
        return None, f"URL Error: {e.reason}"
    except Exception as e:
        return None, f"Error: {str(e)}"

def generate_normal_transaction(user_id):
    tx_id = f"tx_{uuid.uuid4().hex[:10]}"
    # Amount around user average with standard variance
    avg = USER_AVG_SPENT[user_id]
    amount = max(1.0, round(random.normalvariate(avg, avg * 0.3), 2))
    
    # 95% domestic, 5% foreign
    is_foreign = 1 if random.random() < 0.05 else 0
    
    payload = {
        "transaction_id": tx_id,
        "user_id": user_id,
        "amount": amount,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "is_foreign_country": is_foreign
    }
    return payload

def generate_fraud_late_night_foreign(user_id):
    # Rule 1: High amount, foreign, and late night
    tx_id = f"tx_{uuid.uuid4().hex[:10]}"
    amount = round(random.uniform(250, 800), 2)
    
    # Force late night hour in ISO timestamp (between 11 PM and 5 AM)
    current_dt = datetime.now(timezone.utc)
    fraud_hour = random.choice([23, 0, 1, 2, 3, 4, 5])
    simulated_dt = current_dt.replace(hour=fraud_hour, minute=random.randint(0, 59))
    
    payload = {
        "transaction_id": tx_id,
        "user_id": user_id,
        "amount": amount,
        "timestamp": simulated_dt.isoformat().replace("+00:00", "Z"),
        "is_foreign_country": 1
    }
    return payload

def generate_fraud_extreme_amount(user_id):
    # Rule 3: Extreme transaction relative to average
    tx_id = f"tx_{uuid.uuid4().hex[:10]}"
    avg = USER_AVG_SPENT[user_id]
    amount = round(avg * random.uniform(8.0, 15.0), 2)
    amount = max(amount, 160.0) # ensure it is above $150 threshold in trainer rules
    
    payload = {
        "transaction_id": tx_id,
        "user_id": user_id,
        "amount": amount,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "is_foreign_country": 0
    }
    return payload

def main():
    print("====================================================")
    print("      Real-Time Transaction Traffic Simulator        ")
    print("====================================================")
    print(f"Targeting Transaction API at: {API_URL}")
    print("Generating simulated transaction traffic...")
    print("Press Ctrl+C to stop.\n")
    
    tx_count = 0
    
    try:
        while True:
            # Select random user
            user_id = random.choice(USER_IDS)
            
            # Determine transaction type
            rand_val = random.random()
            
            if rand_val < 0.93:
                # Normal transaction
                payload = generate_normal_transaction(user_id)
                tx_type = "NORMAL"
            elif rand_val < 0.96:
                # Fraud Type A: Late night foreign spending
                payload = generate_fraud_late_night_foreign(user_id)
                tx_type = "FRAUD_LATE_NIGHT_FOREIGN"
            elif rand_val < 0.98:
                # Fraud Type B: Large transaction relative to average
                payload = generate_fraud_extreme_amount(user_id)
                tx_type = "FRAUD_EXTREME_AMOUNT"
            else:
                # Fraud Type C: Velocity Attack (generate 6 transactions in rapid succession)
                print(f"[!] Simulating Velocity Attack for user {user_id}...")
                for i in range(6):
                    tx_id = f"tx_vel_{uuid.uuid4().hex[:8]}"
                    amount = round(random.uniform(5, 50), 2)
                    payload = {
                        "transaction_id": tx_id,
                        "user_id": user_id,
                        "amount": amount,
                        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                        "is_foreign_country": 0
                    }
                    res, err = send_transaction(payload)
                    if err:
                        print(f"  └ [ERROR] (Velocity tx {i+1}/6): {err}")
                    else:
                        print(f"  └ [SUCCESS] (Velocity tx {i+1}/6) ID: {tx_id} - ${amount:.2f}")
                    time.sleep(0.05)
                print()
                continue
            
            # Send single transaction
            res, err = send_transaction(payload)
            tx_count += 1
            
            if err:
                print(f"[{tx_count}] [ERROR] [{tx_type}] User: {user_id} | Send Failed: {err}")
            else:
                print(f"[{tx_count}] [SENT]  [{tx_type}] ID: {payload['transaction_id']} | User: {user_id} | Amount: ${payload['amount']:.2f} | Foreign: {payload['is_foreign_country']}")
                
            # Inter-transaction delay (0.2s - 1.2s)
            time.sleep(random.uniform(0.2, 1.2))
            
    except KeyboardInterrupt:
        print("\nSimulator stopped by user.")
    print("====================================================")

if __name__ == "__main__":
    main()
