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

# Define transaction generator as a lambda function
generate_transaction = lambda: {
    "transaction_id": str(uuid.uuid4()),
    "user_id": random.randint(1000, 9999),
    "amount": round(random.uniform(1, 5000), 2),
    "timestamp": datetime.utcnow().isoformat(),
    "merchant_category": random.choice([
        "Electronics", "Groceries", "Clothing", "Gas Station", "Online Purchase",
        "Subscription Service", "Luxury Goods", "Fast Food", "Travel", "Entertainment"
    ]),
    "location": random.choice(["New York", "Los Angeles", "Chicago", "Houston", "San Francisco", "Miami", "Seattle"]),
    "payment_method": random.choice(["Credit Card", "Debit Card", "PayPal", "Bitcoin"]),
    "is_fraud": random.choices([0, 1], weights=[0.98, 0.02], k=1)[0]
}

# Continuous data streaming with exception handling
try:
    while True:
        transaction = generate_transaction()  # Generate transaction using lambda
        producer.send('transaction-events', value=transaction)
        print(f"Sent: {json.dumps(transaction, indent=2)}")
        time.sleep(1)  # Send every second
except KeyboardInterrupt:
    print("\nStopping producer...")
finally:
    producer.close()
