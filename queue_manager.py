import redis
import json
import os
from urllib.parse import urlparse

# Parse Redis URL
redis_url = os.getenv("REDIS_URL")
parsed_url = urlparse(redis_url)

# Set up Redis connection
r = redis.Redis(
    host=parsed_url.hostname,
    port=parsed_url.port,
    username=parsed_url.username,
    password=parsed_url.password,
    ssl=True,
    decode_responses=True
)

def add_task_to_queue(task: dict):
    """
    Adds a task to the central queue.

    Args:
        task (dict): A dictionary representing the task data.

    Returns:
        bool: True if the task was added successfully.
    """
    try:
        task_json = json.dumps(task)
        r.rpush("queue:tasks", task_json)
        print(f"Queued task: {task_json}")
        return True
    except Exception as e:
        print(f"Error queuing task: {e}")
        return False

if __name__ == "__main__":
    # Example usage
    add_task_to_queue({
        "task": {
            "discount_code": "111",
            "discount_value": 10,
            "discount_type": "percentage"
        },
        "store_name": "ubercats.shopify.com",
    })
    add_task_to_queue({
        "task": {
            "discount_code": "222",
            "discount_value": 10,
            "discount_type": "percentage"
        },
        "store_name": "disney.shopify.com",
    })
    add_task_to_queue({
        "task": {
            "discount_code": "333",
            "discount_value": 10,
            "discount_type": "percentage"
        },
        "store_name": "ubercats.shopify.com",
    })

