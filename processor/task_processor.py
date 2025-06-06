"""
Task Processor Module

This module contains the core task processing logic for the queue system.
It handles the execution of tasks and provides error handling and logging.
"""

import os
import time
import json
import logging
from typing import Dict, List, Tuple, Any

from processor.discount_manager import process_discount_codes

logger = logging.getLogger(__name__)

IS_DEV = os.getenv("IS_DEV")

MAX_RETRY_ATTEMPTS = 3

async def execute_task(task_data: Dict[str, Any]) -> bool:
    """
    Process a discount task by generating codes and handling the task execution.
    
    Args:
        task_data: Dictionary containing task information including discount and shop data
        
    Returns:
        bool: True if task was processed successfully, False otherwise
    """
    try:
        task_name = task_data.get('task', 'unknown')
        discount_db = task_data.get('discountDB', {})
        discount_created = task_data.get('discountCreated', {})
        shop_data_db = task_data.get('shopDataDB', {})
        
        response = await process_discount_codes(
            task_name,
            shop_data_db['shop'],
            shop_data_db['access_token'],
            discount_created,
            discount_db['discount_id'],
            discount_db['job_id'],
            discount_db['discount_title'],
            discount_db['s3_object_name']
        )

        return response
        
    except Exception as e:
        logger.error(f"Error executing task for store {task_data.get('store_name', 'unknown')}: {str(e)}")
        return False