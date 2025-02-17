# Data Modeling with Apache Cassandra

This project demonstrates the process of building an ETL pipeline and querying a database using Apache Cassandra, focusing on handling denormalized data efficiently. The Jupyter Notebook walks through the steps of data preprocessing, database creation, and execution of specific queries.

## Table of Contents
- [Project Overview](#project-overview)
- [Dependencies](#dependencies)
- [Dataset](#dataset)
- [Steps](#steps)
  - [1. ETL Pipeline](#1-etl-pipeline)
  - [2. Apache Cassandra Data Modeling](#2-apache-cassandra-data-modeling)
  - [3. Query Execution](#3-query-execution)
- [Queries](#queries)
- [Results](#results)
- [How to Run](#how-to-run)
- [License](#license)

## Project Overview
The project processes event data for a music streaming app to create a denormalized dataset, defines a Cassandra database schema to enable efficient queries, and executes queries to answer business questions.

## Dependencies
- Python 3.x
- Libraries: `pandas`, `cassandra-driver`, `numpy`, `csv`, `os`, `glob`

## Dataset
The dataset consists of multiple CSV files containing user activity logs. These are processed into a single denormalized CSV file (`event_datafile_new.csv`) for use in the database.

## Steps

### 1. ETL Pipeline
- **Import Libraries**: Load required Python libraries.
- **Process Dataset**: Combine multiple CSV files into a single dataset.
- **Create Denormalized Data**: Save the processed data into `event_datafile_new.csv`.

### 2. Apache Cassandra Data Modeling
- **Cluster and Session Setup**: Establish a connection to the Cassandra database.
- **Keyspace Creation**: Define and activate a keyspace.
- **Table Creation**: Create tables to handle specific queries.
- **Data Insertion**: Insert data from the denormalized CSV file into the tables.

### 3. Query Execution
Execute the following queries to retrieve meaningful insights:

## Queries

1. **Fetch Artist, Song Title, and Length**
   Retrieve the artist, song title, and song's length for `sessionId = 338` and `itemInSession = 4`.

2. **Fetch Artist, Song, and User Details**
   Retrieve the artist, song title (sorted by `itemInSession`), and the user's first and last name for `userId = 10` and `sessionId = 182`.

3. **Fetch Users Who Listened to a Specific Song**
   Retrieve the first and last names of users who listened to the song `'All Hands Against His Own'`.

## Results
- Efficiently queried data from Cassandra using the denormalized schema.
- Demonstrated the use of composite keys for efficient data retrieval.

## How to Run
1. Install dependencies using `pip install pandas cassandra-driver`.
2. Clone this repository and navigate to the project folder.
3. Open the Jupyter Notebook file and run each cell sequentially.

## License
This project is open-source and available under the MIT License.

