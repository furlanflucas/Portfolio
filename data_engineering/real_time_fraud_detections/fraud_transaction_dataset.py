from kafka import KafkaProducer
import json
import random
import time
import uuid
from datetime import datetime

# Define Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['<MSK_BROKER_URL>'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# List of merchant categories
merchant_categories = [
    "Electronics", "Groceries", "Clothing", "Gas Station", "Online Purchase",
    "Subscription Service", "Luxury Goods", "Fast Food", "Travel", "Entertainment"
]

# List of locations
locations = ["New York", "Los Angeles", "Chicago", "Houston", "San Francisco", "Miami", "Seattle"]

# List of payment methods
payment_methods = ["Credit Card", "Debit Card", "PayPal", "Bitcoin"]

# Function to generate synthetic transaction data
def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.randint(1000, 9999),  # Simulated unique users
        "amount": round(random.uniform(1, 5000), 2),  # Transaction amount between $1 and $5000
        "timestamp": datetime.utcnow().isoformat(),  # ISO format timestamp
        "merchant_category": random.choice(merchant_categories),
        "location": random.choice(locations),
        "payment_method": random.choice(payment_methods),
        "is_fraud": random.choices([0, 1], weights=[0.98, 0.02], k=1)[0]  # 2% fraud rate
    }

# Continuous data streaming
try:
    while True:
        transaction = generate_transaction()
        producer.send('transaction-events', value=transaction)
        print(f"Sent: {json.dumps(transaction, indent=2)}")
        time.sleep(1)  # Send every second
except KeyboardInterrupt:
    print("\nStopping producer...")
finally:
    producer.close()
