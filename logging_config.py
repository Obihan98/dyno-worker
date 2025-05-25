import logging
import sys

def setup_logging():
    # Create a root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Create console handler with formatting
    console_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(formatter)
    
    # Remove any existing handlers to avoid duplicate logs
    root_logger.handlers = []
    
    # Add the handler to the root logger
    root_logger.addHandler(console_handler)
    
    # Allow propagation to ensure child loggers work
    root_logger.propagate = True 