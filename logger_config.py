"""
Logger Configuration Module

This module provides centralized logging configuration for the application.
"""

import sys
import logging

# Create a formatter that includes timestamp, level, module, and message
formatter = logging.Formatter(
    '%(message)s',
)

# Create a handler that writes to stdout
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)

# Configure the root logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# Prevent propagation to avoid duplicate logs
logger.propagate = False 