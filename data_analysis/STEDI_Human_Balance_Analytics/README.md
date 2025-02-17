# STEDI Human Balance Analytics

## 📌 Project Overview
This project is part of Udacity's Data Engineering Nanodegree and focuses on processing IoT sensor data for human balance analytics using AWS services. The pipeline ingests data from S3, processes it with AWS Glue, and queries the results in Athena.

## 🛠 Technologies Used
- **AWS S3**: Storage for raw and processed data.
- **AWS Glue**: ETL job orchestration and schema inference.
- **AWS Athena**: Querying the processed data.
- **Apache Spark**: Data transformations.
- **SQL**: Data schema definition and analysis.

## 📂 Project Structure
```
├── scripts/                     # ETL scripts for AWS Glue
│   ├── customer_landing_to_trusted.py
│   ├── accelerometer_landing_to_trusted.py
│   ├── step_trainer_trusted.py
│   ├── customer_trusted_to_curated.py
│   ├── machine_learning_curated.py
│
├── sql/                         # Schema definition
│   ├── customer_landing.sql
│   ├── accelerometer_landing.sql
│   ├── step_trainer_landing.sql
│
├── screenshots/                 # Athena query results
│   ├── athena_results_landing_zone.png
│   ├── athena_results_trusted_zone.png
│   ├── athena_results_curated_zone.png
│
└── README.md                     # Project documentation
```

## 🔄 ETL Pipeline Workflow
1. **Landing Zone**:
   - Data is ingested into S3 from various IoT devices.
   - Glue crawlers identify schema and store metadata in AWS Glue Data Catalog.

2. **Trusted Zone**:
   - AWS Glue ETL jobs process data and filter out incomplete or incorrect records.
   - Schema is dynamically updated.
   - PII is removed.

3. **Curated Zone**:
   - The final dataset is aggregated and structured for machine learning analysis.
   - The data is stored in a ready-to-query format using AWS Athena.

## 📊 Data Schema
### **Customer Landing Table** (`customer_landing.sql`)
| Column | Type |
|---------|------|
| customer_id | STRING |
| email | STRING |
| registration_date | TIMESTAMP |
| share_with_research | BOOLEAN |

### **Accelerometer Landing Table** (`accelerometer_landing.sql`)
| Column | Type |
|---------|------|
| user | STRING |
| timestamp | BIGINT |
| x | DOUBLE |
| y | DOUBLE |
| z | DOUBLE |

### **Step Trainer Landing Table** (`step_trainer_landing.sql`)
| Column | Type |
|---------|------|
| sensor_reading_time | TIMESTAMP |
| serial_number | STRING |
| distance_from_object | DOUBLE |

## 📜 Querying Data in Athena
Sample SQL queries to validate data:
```sql
-- Check number of customers in trusted dataset
SELECT COUNT(*) FROM customer_trusted;

-- Identify missing shareWithResearchAsOfDate values in landing zone
SELECT COUNT(*) FROM customer_landing WHERE share_with_research IS NULL;

-- Verify total accelerometer readings
SELECT COUNT(*) FROM accelerometer_trusted;
```

## 📌 How to Run the Project
### **1. Setup AWS Glue Tables**
1. Create an **S3 bucket** for storing raw and processed data.
2. Use **AWS Glue Crawler** to infer the schema from JSON data.
3. Create external tables in **AWS Glue Data Catalog** using the provided SQL DDL scripts.

### **2. Run AWS Glue Jobs**
1. Execute `customer_landing_to_trusted.py` to clean customer data.
2. Execute `accelerometer_landing_to_trusted.py` to process sensor data.
3. Execute `step_trainer_trusted.py` to transform step trainer data.
4. Run `customer_trusted_to_curated.py` and `machine_learning_curated.py` to generate final datasets.

### **3. Query Data in Athena**
- Navigate to AWS Athena and select the **stedi** database.
- Run queries on **customer_trusted, accelerometer_trusted, and curated datasets**.
- Validate results using the provided screenshots.

## 📌 Results & Insights
- Successfully cleaned and processed IoT data.
- Filtered out **PII and inconsistent records**.
- Created **machine-learning-ready datasets** for further analysis.

## 📎 Resources
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html)
- [AWS Athena Documentation](https://docs.aws.amazon.com/athena/latest/ug/what-is.html)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)

## ✍️ Author
- **Lucas Furlan**
- GitHub: [furlanflucas](https://github.com/furlanflucas)

