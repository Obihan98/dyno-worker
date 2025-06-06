import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv
import os
import logging
from typing import List, Dict, Any, Optional
from contextlib import contextmanager

# Get logger for this module
logger = logging.getLogger(__name__)

IS_DEV = os.getenv("IS_DEV")

# Load environment variables
load_dotenv()

# Get database connection details from environment variables
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Create a connection pool
connection_pool = pool.SimpleConnectionPool(
    1,  # minconn
    10,  # maxconn
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)

@contextmanager
def get_db_connection():
    """
    Context manager for database connections.
    Ensures connections are properly released back to the pool.
    """
    conn = None
    try:
        conn = connection_pool.getconn()
        yield conn
    finally:
        if conn is not None:
            connection_pool.putconn(conn)

def execute_query(query, params=None):
    """
    Execute a database query with proper connection management.
    
    Args:
        query (str): The SQL query to execute
        params (tuple, optional): Parameters for the query
        
    Returns:
        list: Query results
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            if cursor.description:  # If the query returns results
                return cursor.fetchall()
            return None

def release_connection(conn):
    """
    Release a connection back to the pool
    """
    if conn is not None:
        connection_pool.putconn(conn)
