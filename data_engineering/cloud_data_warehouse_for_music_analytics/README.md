# Cloud Data Warehouse for Music Analytics

## Project Overview
This project demonstrates the creation of a scalable, cloud-based data warehouse using AWS Redshift. The primary goal is to process and analyze song play data collected from user logs and song metadata. The project implements a star schema optimized for analytical queries and an ETL pipeline to efficiently extract, transform, and load data from Amazon S3 into Redshift.

## Table of Contents
- [Project Structure](#project-structure)
- [Technologies Used](#technologies-used)
- [Schema Design](#schema-design)
- [ETL Pipeline](#etl-pipeline)
- [Setup and Execution](#setup-and-execution)
- [Key Insights](#key-insights)
- [License](#license)

## Project Structure
- **create_tables.py**: Script to initialize the database by dropping existing tables and creating new ones based on the star schema.
- **etl.py**: Implements the ETL pipeline to extract data from S3, stage it in Redshift, and populate analytics tables.
- **dwh.cfg**: Configuration file containing Redshift connection parameters, IAM role details, and S3 data paths.
- **sql_queries.py**: Contains SQL queries for creating, dropping, copying, and inserting data into tables.
- **README.md**: Documentation for the project.

## Technologies Used
- **AWS Redshift**: Cloud-based data warehouse for scalable data storage and analysis.
- **Amazon S3**: Source of raw log and song metadata files.
- **Python**: Programming language for scripting the ETL pipeline.
- **Psycopg2**: Library for connecting and executing queries on Redshift.
- **ConfigParser**: Used for managing configuration settings.

## Schema Design
### Star Schema
- **Fact Table**:
  - `songplays`: Records of song plays, including timestamps and user interactions.
  
- **Dimension Tables**:
  - `users`: Information about users (e.g., name, gender, subscription level).
  - `songs`: Metadata about songs (e.g., title, artist, duration).
  - `artists`: Details about artists (e.g., name, location, coordinates).
  - `time`: Breakdown of timestamps into components like hour, day, and week.

### Diagram
```
            songplays
           /   |   |   \
      users  songs  artists  time
```

## ETL Pipeline
1. **Extract**: Copies raw data from S3 into Redshift staging tables using `COPY` commands.
2. **Transform**: Processes the staging tables to generate normalized fact and dimension tables.
3. **Load**: Inserts processed data into the final star schema tables for analysis.

## Setup and Execution
### Prerequisites
- Python 3.x
- AWS Redshift cluster with appropriate IAM roles and permissions.

### Steps
1. **Update Configuration**:
   - Modify `dwh.cfg` with your Redshift cluster endpoint, IAM role ARN, and S3 paths.

2. **Initialize Database**:
   - Run `create_tables.py` to drop existing tables and create new ones.

   ```bash
   python create_tables.py
   ```

3. **Run ETL Pipeline**:
   - Execute `etl.py` to load data into Redshift and populate analytics tables.

   ```bash
   python etl.py
   ```

4. **Verify Results**:
   - Use SQL queries to analyze the data stored in the analytics tables.

## Key Insights
- Built a star schema to facilitate analytical queries on song play data.
- Processed raw data from S3 into structured tables using Redshift's staging mechanism.
- Enabled scalable and efficient querying of user activity and song metadata.

## License
This project is open-source and licensed under the MIT License.

