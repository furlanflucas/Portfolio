import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops all existing tables in the database.

    Parameters:
    cur: cursor object for executing queries
    conn: connection object for the database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates all tables in the database as per the schema.

    Parameters:
    cur: cursor object for executing queries
    conn: connection object for the database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function to connect to Redshift, drop existing tables, and create new ones.
    """
    # Redshift connection details
    HOST = "redshift-cluster-1.chbcrbohx5rx.us-east-1.redshift.amazonaws.com:5439/dev"  
    DB_NAME = "dev"
    DB_USER = "user"
    DB_PASSWORD = "password" 
    DB_PORT = "5439"

    # Connect to the Redshift cluster
    try:
        conn = psycopg2.connect(
            host=HOST,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cur = conn.cursor()
        print("Connected to Redshift cluster")

        # Drop and recreate tables
        drop_tables(cur, conn)
        print("Dropped tables if they existed")

        create_tables(cur, conn)
        print("Created tables as per the schema")

        # Close the connection
        conn.close()
        print("Connection closed")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()

