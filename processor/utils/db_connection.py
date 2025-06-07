from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os
import logging
from typing import Generator, Optional
from contextlib import contextmanager

# Get logger for this module
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get database connection details from environment variables
DB_CREDENTIALS = os.getenv("DB_CREDENTIALS", "")

# Parse the credentials string
if DB_CREDENTIALS:
    try:
        DB_USER, DB_PASSWORD, DB_HOST, DB_NAME = DB_CREDENTIALS.split(":")
        DB_PORT = "5432"  # Default PostgreSQL port
    except ValueError:
        logger.error("Invalid DB_CREDENTIALS format. Expected format: username:password:host:database")
        raise ValueError("Invalid DB_CREDENTIALS format")

# Create database URL
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Create engine with connection pooling
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=5,  # Maximum number of connections to keep
    max_overflow=10,  # Maximum number of connections that can be created beyond pool_size
    pool_timeout=30,  # Seconds to wait before giving up on getting a connection from the pool
    pool_recycle=1800,  # Recycle connections after 30 minutes
    pool_pre_ping=True,  # Enable connection health checks
    echo=False  # Set to True for SQL query logging
)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@contextmanager
def get_db_session() -> Generator[Session, None, None]:
    """
    Context manager for database sessions.
    Ensures sessions are properly closed and transactions are handled correctly.
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Database session error: {str(e)}")
        raise
    finally:
        session.close()

def execute_query(query: str, params: Optional[dict] = None) -> Optional[list]:
    """
    Execute a database query with proper session management.
    
    Args:
        query (str): The SQL query to execute
        params (dict, optional): Parameters for the query
        
    Returns:
        list: Query results
    """
    try:
        with get_db_session() as session:
            # logger.info(f"Executing query: {query}")
            # if params:
            #     logger.info(f"Query parameters: {params}")
            
            # Convert the query string to a SQLAlchemy text object
            sql_text = text(query)
            result = session.execute(sql_text, params)
            
            if result.returns_rows:
                results = result.fetchall()
                logger.info(f"Query returned {len(results)} results")
                return results
            
            logger.info("Query executed successfully")
            return None
    except SQLAlchemyError as e:
        logger.error(f"Database error executing query: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in execute_query: {str(e)}")
        raise
