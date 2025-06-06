"""
Distributed Task Queue System

This module implements a distributed task queue system using Redis as the message broker.
It provides functionality for task distribution, processing, and store management.

The system consists of:
- A central dispatcher that routes tasks to appropriate store workers
- Store-specific worker threads that process tasks
- Automatic cleanup of inactive stores
- Retry mechanism for failed tasks
"""

import redis
import json
import os
import time
import threading
import asyncio
import logging
from typing import Dict, List, Set
from threading import Lock
from queue import Queue, Empty
from collections import defaultdict
from datetime import datetime, timedelta
from processor.task_processor import execute_task
from dotenv import load_dotenv
import pytz

# Configure logging with US Eastern Time
class EasternTimeFormatter(logging.Formatter):
    def converter(self, timestamp):
        dt = datetime.fromtimestamp(timestamp)
        eastern = pytz.timezone('US/Eastern')
        return dt.astimezone(eastern)
    
    def formatTime(self, record, datefmt=None):
        dt = self.converter(record.created)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime('%Y-%m-%d %H:%M:%S %Z')

# Configure root logger
logging.basicConfig(level=logging.DEBUG)  # Change to DEBUG level
logger = logging.getLogger(__name__)

# Set the formatter for all handlers
formatter = EasternTimeFormatter('%(asctime)s - %(levelname)s - %(message)s')
for handler in logging.getLogger().handlers:
    handler.setFormatter(formatter)

# Load environment variables from .env file
load_dotenv()

# Parse Redis URL
REDIS_URL = os.getenv("REDIS_URL")
IS_DEV = os.getenv("IS_DEV") == "True"

if not REDIS_URL:
    raise ValueError("REDIS_URL environment variable is not set")

# Add file handler for persistent logging
file_handler = logging.FileHandler('app.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

class RedisConnectionManager:
    def __init__(self, redis_url: str, max_retries: int = 3, retry_delay: int = 5):
        self.redis_url = redis_url
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.redis = None
        self.connect()

    def connect(self) -> None:
        """Establish connection to Redis with retry mechanism."""
        retries = 0
        while retries < self.max_retries:
            try:
                self.redis = redis.from_url(self.redis_url, decode_responses=True)
                self.redis.ping()  # Test the connection
                logger.info("Successfully connected to Redis")
                return
            except redis.ConnectionError as e:
                retries += 1
                logger.error(f"Failed to connect to Redis (attempt {retries}/{self.max_retries}): {e}")
                if retries < self.max_retries:
                    time.sleep(self.retry_delay)
                else:
                    raise

    def ensure_connection(self) -> redis.Redis:
        """Ensure Redis connection is alive, reconnect if necessary."""
        try:
            self.redis.ping()
            return self.redis
        except (redis.ConnectionError, redis.ResponseError):
            logger.warning("Redis connection lost, attempting to reconnect...")
            self.connect()
            return self.redis

# Initialize Redis connection manager
redis_manager = RedisConnectionManager(REDIS_URL)

# Global state for store management
store_locks = defaultdict(threading.Lock)
store_queues = defaultdict(Queue)
active_stores: Set[str] = set()
active_stores_lock = Lock()
store_last_activity = defaultdict(lambda: datetime.now())
store_processing = defaultdict(bool)  # Track if store is currently processing a task
store_processing_lock = Lock()

# Configuration constants
STORE_INACTIVITY_TIMEOUT = 3600  # 1 hour
MAX_RETRIES = 1
RETRY_DELAY = 5  # seconds
ACTIVITY_UPDATE_INTERVAL = 300  # Update activity every 5 minutes during long tasks

def process_store_task(store_name: str, task: dict) -> bool:
    """
    Process a task for a specific store with retry mechanism.
    
    Args:
        store_name (str): The name of the store to process the task for
        task (dict): The task data to process
        
    Returns:
        bool: True if the task was processed successfully, False otherwise
    """
    logger.info(f"Starting to process task for store {store_name}")
    try:
        logger.debug(f"Task data: {json.dumps(task, indent=2)}")
    except Exception as e:
        logger.error(f"Error logging task data: {e}")
    
    # Mark store as processing
    with store_processing_lock:
        store_processing[store_name] = True
        store_last_activity[store_name] = datetime.now()
    
    try:
        retries = 0
        while retries < MAX_RETRIES:
            try:
                logger.info(f"Creating new event loop for store {store_name}")
                # Create a new event loop for this thread
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                # Start a background task to update activity periodically
                async def update_activity():
                    while store_processing[store_name]:
                        store_last_activity[store_name] = datetime.now()
                        await asyncio.sleep(ACTIVITY_UPDATE_INTERVAL)
                
                # Create and start the activity updater task
                activity_updater = loop.create_task(update_activity())
                
                try:
                    logger.info(f"Executing task for store {store_name}")
                    # Execute the task using the task processor
                    success = loop.run_until_complete(execute_task(task))
                    
                    if success:
                        logger.info(f"Successfully processed task for store {store_name}")
                        return True
                    else:
                        logger.error(f"Task execution failed for store {store_name}")
                        raise Exception("Task execution failed")
                except Exception as e:
                    logger.error(f"Error in task execution: {str(e)}")
                    raise
                finally:
                    # Cancel the activity updater and wait for it to complete
                    activity_updater.cancel()
                    try:
                        loop.run_until_complete(activity_updater)
                    except asyncio.CancelledError:
                        pass
                    loop.close()
                    
            except Exception as e:
                retries += 1
                logger.error(f"Error processing task for store {store_name} (attempt {retries}/{MAX_RETRIES}): {e}")
                try:
                    logger.error(f"Task that caused error: {json.dumps(task, indent=2)}")
                except Exception as json_error:
                    logger.error(f"Error logging task data: {json_error}")
                
                if retries < MAX_RETRIES:
                    logger.info(f"Retrying task for store {store_name} in {RETRY_DELAY} seconds...")
                    time.sleep(RETRY_DELAY)
                else:
                    logger.error(f"Max retries reached for store {store_name}. Task failed.")
                    return False
    finally:
        # Mark store as not processing
        with store_processing_lock:
            store_processing[store_name] = False
            logger.info(f"Finished processing task for store {store_name}")

def cleanup_inactive_stores():
    """
    Clean up stores that have been inactive for too long.
    Only cleans up stores that are not currently processing tasks.
    """
    current_time = datetime.now()
    stores_to_remove = set()
    
    with store_processing_lock:
        for store_name, last_activity in store_last_activity.items():
            # Only clean up if store is not processing and has been inactive
            if (not store_processing[store_name] and 
                (current_time - last_activity) > timedelta(seconds=STORE_INACTIVITY_TIMEOUT)):
                stores_to_remove.add(store_name)
    
    if stores_to_remove:
        with active_stores_lock:
            for store_name in stores_to_remove:
                if store_name in active_stores:
                    active_stores.remove(store_name)
                    del store_last_activity[store_name]
                    del store_processing[store_name]
                    logger.info(f"Cleaned up inactive store: {store_name}")

def store_worker(store_name: str):
    """
    Worker thread that processes tasks for a specific store.
    
    Args:
        store_name (str): The name of the store to process tasks for
    """
    logger.info(f"Starting new thread for {store_name}")
    
    while store_name in active_stores:
        try:            # Get task from store's queue with timeout
            try:
                task = store_queues[store_name].get(timeout=1)
            except Empty:
                continue
            
            # Process the task
            success = process_store_task(store_name, task)
            
            # Mark task as done
            store_queues[store_name].task_done()
                
        except Exception as e:
            logger.error(f"Error in store worker {store_name}: {e}")
            time.sleep(1)  # Wait before retrying

def dispatcher():
    """
    Central dispatcher that routes tasks to appropriate store workers.
    """
    logger.info("Starting dispatcher...")
    
    while True:
        try:
            # Ensure Redis connection is alive before attempting to get tasks
            r = redis_manager.ensure_connection()
            
            # Blocking pop from the central queue with a timeout of 1 second
            task_data = r.blpop("queue:tasks", timeout=1)
            if task_data:
                try:
                    task = json.loads(task_data[1])
                    store_name = task.get("discountDB", {}).get("shop")
                    
                    if not store_name:
                        logger.error(f"Invalid task data - missing shop name: {task_data[1]}")
                        continue

                    logger.info(f"Received task for store: {store_name}")
                    logger.debug(f"Task data: {json.dumps(task, indent=2)}")
                    
                    # Add task to store's queue
                    store_queues[store_name].put(task)
                    
                    # Start a new worker if this store doesn't have one
                    with active_stores_lock:
                        if store_name not in active_stores:
                            thread = threading.Thread(
                                target=store_worker,
                                args=(store_name,),
                                daemon=True
                            )
                            thread.start()
                            active_stores.add(store_name)
                            store_last_activity[store_name] = datetime.now()
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding task data: {e}")
                    logger.error(f"Raw task data: {task_data[1]}")
                except Exception as e:
                    logger.error(f"Error processing task data: {e}")
            
            # Periodically clean up inactive stores
            cleanup_inactive_stores()
                
        except redis.ConnectionError as e:
            logger.error(f"Redis connection error in dispatcher: {e}")
            # Force a reconnection attempt
            redis_manager.connect()
            time.sleep(1)  # Wait before retrying
        except redis.ResponseError as e:
            logger.error(f"Redis response error in dispatcher: {e}")
            time.sleep(1)  # Wait before retrying
        except Exception as e:
            logger.error(f"Unexpected error in dispatcher: {e}")
            logger.exception("Full traceback:")  # This will log the full stack trace
            time.sleep(1)  # Wait before retrying

if __name__ == "__main__":
    # Start the dispatcher
    dispatcher()