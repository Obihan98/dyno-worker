"""
Logger Configuration Module

This module provides centralized logging configuration for the application.
"""

import sys
import logging
import queue
import threading
from logging.handlers import QueueHandler, QueueListener

# Create a queue for thread-safe logging
log_queue = queue.Queue()
queue_handler = QueueHandler(log_queue)

# Create a formatter that includes timestamp, level, thread name, and message
formatter = logging.Formatter(
    '%(message)s',
)

# Create a handler that writes to stdout with immediate flushing
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)
handler.setLevel(logging.INFO)

# Create a queue listener
queue_listener = QueueListener(log_queue, handler, respect_handler_level=True)
queue_listener.start()

# Configure the root logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(queue_handler)

# Prevent propagation to avoid duplicate logs
logger.propagate = False

# Ensure all handlers flush immediately
for handler in logger.handlers:
    handler.flush = lambda: True 