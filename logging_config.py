"""
Logging Configuration Module

This module provides a simple logging configuration for the application.
"""

import logging

def setup_logging():
    """
    Set up basic logging configuration for the application.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ) 

    # Configure root logger to propagate messages
    root_logger = logging.getLogger()
    root_logger.propagate = True
