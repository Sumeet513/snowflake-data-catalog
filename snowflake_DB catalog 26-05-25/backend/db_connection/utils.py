import logging
import sys

def setup_logger():
    logger = logging.getLogger('snowflake_process')
    logger.setLevel(logging.INFO)
    
    # Console handler with custom formatting
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # Create a custom formatter
    formatter = logging.Formatter(
        '\n%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(console_handler)
    return logger

process_logger = setup_logger()