import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv
import os

IS_DEV = os.getenv("IS_DEV")

# Load environment variables
load_dotenv()

# Get database credentials from environment variable
DB_CREDENTIALS = os.getenv('DB_CREDENTIALS')
user, password, host, database = DB_CREDENTIALS.split(':')

# Create a connection pool
connection_pool = pool.ThreadedConnectionPool(
    minconn=1,
    maxconn=10,
    user=user,
    host=host,
    database=database,
    password=password,
    port=5432,
    sslmode='require',
    sslrootcert=None  # This is equivalent to rejectUnauthorized: false in Node.js
)

def get_connection():
    """
    Get a connection from the pool
    """
    return connection_pool.getconn()

def release_connection(conn):
    """
    Release a connection back to the pool
    """
    connection_pool.putconn(conn)

def execute_query(query: str, params):
    """
    Execute a query and return the results
    """
    conn = None
    try:
        if not DB_CREDENTIALS:
            print("Error: DB_CREDENTIALS environment variable is not set", flush=True)
            return None
            
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            conn.commit()  # Commit the transaction
            if cursor.description:  # If the query returns data
                results = cursor.fetchall()
                return results
            return None
    except Exception as e:
        print(f"Database Error: {str(e)}", flush=True)
        print(f"Query: {query}", flush=True)
        print(f"Params: {params}", flush=True)
        if conn:
            conn.rollback()  # Rollback on error
    finally:
        if conn:
            release_connection(conn)
