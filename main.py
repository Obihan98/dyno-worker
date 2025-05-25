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
import sys
import threading
from urllib.parse import urlparse
from typing import Dict, List, Set
from threading import Lock
from queue import Queue, Empty
from collections import defaultdict
from datetime import datetime, timedelta
from task_processor import execute_task

# Parse Redis URL
redis_url = os.getenv("REDIS_URL")
if not redis_url:
    raise ValueError("REDIS_URL environment variable is not set")

# Set up Redis connection
try:
    # For Render internal Redis, we can connect directly using the URL
    r = redis.from_url(redis_url, decode_responses=True)
    # Test the connection
    r.ping()
    print("Successfully connected to Redis", flush=True)
except redis.ConnectionError as e:
    print(f"Failed to connect to Redis: {e}", flush=True)
    raise

# Global state for store management
store_locks = defaultdict(threading.Lock)
store_queues = defaultdict(Queue)
active_stores: Set[str] = set()
active_stores_lock = Lock()
store_last_activity = defaultdict(lambda: datetime.now())

# Configuration constants
STORE_INACTIVITY_TIMEOUT = 300  # 5 minutes
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

def process_store_task(store_name: str, task: dict) -> bool:
    """
    Process a task for a specific store with retry mechanism.
    
    Args:
        store_name (str): The name of the store to process the task for
        task (dict): The task data to process
        
    Returns:
        bool: True if the task was processed successfully, False otherwise
    """
    retries = 0
    while retries < MAX_RETRIES:
        try:
            print(f"Processing task for {store_name}", flush=True)
            
            # Execute the task using the task processor
            success = execute_task(task)
            
            if success:
                store_last_activity[store_name] = datetime.now()
                return True
            else:
                raise Exception("Task execution failed")
                
        except Exception as e:
            retries += 1
            print(f"Error processing task for store {store_name} (attempt {retries}/{MAX_RETRIES}): {e}", flush=True)
            print(f"Task that caused error: {task}", flush=True)
            
            if retries < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                print(f"Failed to process task for store {store_name} after {MAX_RETRIES} attempts", flush=True)
                return False

def cleanup_inactive_stores():
    """
    Clean up stores that have been inactive for too long.
    """
    current_time = datetime.now()
    stores_to_remove = set()
    
    for store_name, last_activity in store_last_activity.items():
        if (current_time - last_activity) > timedelta(seconds=STORE_INACTIVITY_TIMEOUT):
            stores_to_remove.add(store_name)
    
    if stores_to_remove:
        with active_stores_lock:
            for store_name in stores_to_remove:
                if store_name in active_stores:
                    active_stores.remove(store_name)
                    del store_last_activity[store_name]
                    print(f"Cleaned up inactive store: {store_name}", flush=True)

def store_worker(store_name: str):
    """
    Worker thread that processes tasks for a specific store.
    
    Args:
        store_name (str): The name of the store to process tasks for
    """    
    while store_name in active_stores:
        try:
            # Get task from store's queue with timeout
            try:
                task = store_queues[store_name].get(timeout=1)
            except Empty:
                continue
            
            # Process the task
            success = process_store_task(store_name, task)
            
            # Mark task as done
            store_queues[store_name].task_done()
            
            if not success:
                # If task failed after all retries, log it for manual review
                print(f"Task failed for store {store_name}: {task}", flush=True)
                
        except Exception as e:
            print(f"Error in store worker {store_name}: {e}", flush=True)
            time.sleep(1)  # Wait before retrying

def dispatcher():
    """
    Central dispatcher that routes tasks to appropriate store workers.
    """
    print("Starting dispatcher...", flush=True)
    
    while True:
        try:
            # Blocking pop from the central queue with a timeout of 1 second
            task_data = r.blpop("queue:tasks", timeout=1)
            if task_data:
                task = json.loads(task_data[1])
                store_name = task.get("store_name", "default")
                                
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
            
            # Periodically clean up inactive stores
            cleanup_inactive_stores()
                
        except Exception as e:
            print(f"Error in dispatcher: {e}", flush=True)
            time.sleep(1)  # Wait before retrying

if __name__ == "__main__":
    # Start the dispatcher
    dispatcher()