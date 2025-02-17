import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Executes the COPY commands to load data from S3 into Redshift staging tables.

    Parameters:
    cur: psycopg2 cursor object to execute database queries.
    conn: psycopg2 connection object for the Redshift database.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Executes the INSERT commands to populate analytics tables from staging tables.

    Parameters:
    cur: psycopg2 cursor object to execute database queries.
    conn: psycopg2 connection object for the Redshift database.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Orchestrates the ETL process:
    1. Reads configuration from the 'dwh.cfg' file.
    2. Connects to the Redshift cluster.
    3. Loads data into staging tables by calling the `load_staging_tables` function.
    4. Populates analytics tables by calling the `insert_tables` function.
    5. Closes the database connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        host="redshift-cluster-1.chbcrbohx5rx.us-east-1.redshift.amazonaws.com:5439/dev",  
        dbname="dev", 
        user="user",
        password="password",  
        port="5439"
    )
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
