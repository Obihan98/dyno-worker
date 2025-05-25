"""
Logger Configuration Module

This module provides centralized logging configuration for the application.
"""

import sys
import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__) 