import logging
import sys

def setup_logging():
    # Get the root logger
    root_logger = logging.getLogger()
    
    # Only set up logging if it hasn't been set up before
    if not root_logger.handlers:
        root_logger.setLevel(logging.INFO)
        
        # Create console handler with formatting
        console_handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(message)s')
        console_handler.setFormatter(formatter)
        
        # Add the handler to the root logger
        root_logger.addHandler(console_handler)
        
        # Allow propagation to ensure child loggers work
        root_logger.propagate = True 