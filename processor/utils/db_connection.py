import os
import logging
from typing import Optional, Any, List, Dict
import psycopg2
from psycopg2 import pool
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database connection pool
connection_pool = None

# Get database credentials from environment
DB_CREDENTIALS = os.getenv("DB_CREDENTIALS")

def init_connection_pool():
    """Initialize the database connection pool"""
    global connection_pool
    try:
        if not DB_CREDENTIALS:
            logger.error("Error: DB_CREDENTIALS environment variable is not set")
            return None
            
        connection_pool = psycopg2.pool.SimpleConnectionPool(
            1,  # minconn
            10,  # maxconn
            DB_CREDENTIALS
        )
        logger.info("Database connection pool initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing database connection pool: {str(e)}")
        raise

def get_connection():
    """Get a connection from the pool"""
    if not connection_pool:
        init_connection_pool()
    return connection_pool.getconn()

def release_connection(conn):
    """Release a connection back to the pool"""
    if connection_pool:
        connection_pool.putconn(conn)

def execute_query(query: str, params) -> Optional[List[Any]]:
    """
    Execute a query and return the results
    """
    conn = None
    try:
        if not DB_CREDENTIALS:
            logger.error("Error: DB_CREDENTIALS environment variable is not set")
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
        logger.error(f"Database Error: {str(e)}")
        logger.debug(f"Query: {query}")
        logger.debug(f"Params: {params}")
        if conn:
            conn.rollback()  # Rollback on error
    finally:
        if conn:
            release_connection(conn)
