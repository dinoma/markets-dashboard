import logging
from datetime import datetime
from typing import Dict, Any

def setup_logging():
    """Configure error logging"""
    logging.basicConfig(
        level=logging.ERROR,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def log_error(error_info: Dict[str, Any]):
    """Log error information with additional context"""
    logger = logging.getLogger('error_boundary')
    logger.error(
        "Error in component %s: %s\nStack trace: %s\nTimestamp: %s",
        error_info.get('component', 'Unknown'),
        error_info.get('error', 'Unknown error'),
        error_info.get('info', {}).get('componentStack', 'No stack'),
        error_info.get('timestamp', 'Unknown time')
    )

def report_error(error_info: Dict[str, Any]):
    """Report error to monitoring service"""
    # TODO: Implement error reporting integration
    pass

# Initialize logging when module is loaded
setup_logging()
