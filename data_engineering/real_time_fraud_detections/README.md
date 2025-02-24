# 🚀 Real-Time Fraud Detection with AWS, Kafka, Spark, and Machine Learning  

## 🚧 Project Status: In Development
*Note: This project is still being built. Features and components may be subject to change.*

## 📌 Project Overview  
This project demonstrates how to detect fraudulent transactions **in real-time** using **AWS services, Apache Kafka (MSK), Apache Spark (EMR), and SageMaker for ML model inference**. The goal is to provide a working pipeline that ingests, processes, classifies, and stores transaction data efficiently.  

## 🎯 Objectives  
✅ **Ingest streaming transaction data** using Apache Kafka (MSK).  
✅ **Process transaction streams** with Apache Spark on AWS EMR.  
✅ **Apply a trained machine learning model** to classify transactions as fraudulent or not.  
✅ **Store fraud detection results** in a database (AWS RDS or DynamoDB).  
✅ **Trigger alerts** for detected fraud using AWS Lambda.  

---

## 🛠 Tech Stack  
| Technology  | Purpose  |
|------------|--------------------------|
| **AWS MSK (Kafka)** | Real-time data ingestion  |
| **AWS EMR (Spark Streaming)** | Processing Kafka streams  |
| **AWS S3** | Storing raw and processed data  |
| **AWS SageMaker** | ML model training & deployment  |
| **AWS Lambda** | Triggering alerts on fraud detection  |
| **AWS RDS / DynamoDB** | Storing fraud detection results  |

---

## 📌 Steps to Build the Project  

### 🏁 Step 1: Set Up Kafka (AWS MSK)  
Kafka is used to **collect and distribute** transaction data streams to different consumers.  

#### 🛠 **Create an MSK Cluster**  
1. Navigate to AWS Console → **MSK** → **Create Cluster**.  
2. Select **"Custom Create"**, and use **"Provisioned"** for Cluster Type.  
3. Choose at least **3 broker nodes** (e.g., `t3.small` for testing).  
4. Enable **public access** for easier testing.  
5. Click **Create Cluster**.  

#### 🛠 **Create a Kafka Topic**  
```bash
kafka-topics.sh --create --zookeeper <zookeeper_connect_string> \
--replication-factor 1 --partitions 3 --topic transaction-events
```
📌 This topic (`transaction-events`) will hold transaction data.  

#### 🛠 **Produce Transaction Data**  
```python
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
```
📌 Replace `<MSK_BROKER_URL>` with your MSK broker address.  

---

### 🚀 Step 2: Process Streaming Data with Spark on EMR  
Apache Spark Streaming allows us to process real-time transaction events.  

#### 🛠 **Launch an EMR Cluster**  
1. Go to **AWS EMR** → **Create Cluster**.  
2. Choose **"Go to Advanced Options"**.  
3. Select **Spark** as the application.  
4. Choose an instance type like **m5.xlarge**.  
5. Enable **Auto-terminate** (optional for cost control).  
6. Click **Create Cluster**.  

#### 🛠 **Write a Spark Streaming Script**  
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, IntegerType, DoubleType, LongType
from pyspark.ml.classification import LogisticRegressionModel

spark = SparkSession.builder.appName("FraudDetection").getOrCreate()

schema = StructType() \
    .add("transaction_id", IntegerType()) \
    .add("user_id", IntegerType()) \
    .add("amount", DoubleType()) \
    .add("timestamp", LongType()) \
    .add("is_fraud", IntegerType())

transactions = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "<MSK_BROKER_URL>") \
    .option("subscribe", "transaction-events") \
    .load()

transaction_df = transactions \
    .selectExpr("CAST(value AS STRING)") \
    .selectExpr("json_tuple(value, 'transaction_id', 'user_id', 'amount', 'timestamp', 'is_fraud') as (transaction_id, user_id, amount, timestamp, is_fraud)") \
    .select(col("transaction_id").cast(IntegerType()),
            col("user_id").cast(IntegerType()),
            col("amount").cast(DoubleType()),
            col("timestamp").cast(LongType()),
            col("is_fraud").cast(IntegerType()))

model = LogisticRegressionModel.load("s3://your-bucket/fraud_model")
predictions = model.transform(transaction_df)

query = predictions.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
```
📌 Replace `<MSK_BROKER_URL>` with your broker details.  

#### 🛠 **Submit the Spark Job**  
```bash
spark-submit fraud_detection_spark.py
```
## 📌 Next Steps  
✅ Automate model retraining with SageMaker Pipelines.  
✅ Optimize Spark performance with partitioning.  
✅ Implement Lambda alerts for high fraud detection.  

---

## 📌 Conclusion  
This project demonstrates **real-time fraud detection** by integrating **Kafka, Spark Streaming, and AWS SageMaker**. The system **ingests, processes, classifies, and stores** transaction data with fraud predictions.  

## 📩 Contact & Contributions  
💡 **Feel free to fork, contribute, or reach out for discussions!** 🚀  
