import logging
import os
import sys

from logging import StreamHandler
from logging.handlers import RotatingFileHandler
from pathlib import Path

log_file_path = Path(os.getcwd()).joinpath('logs', 'analytics.log')

logging.basicConfig(
    handlers=[
        RotatingFileHandler(
            log_file_path,
            maxBytes=10240000,
            backupCount=3,
        ),
        StreamHandler(sys.stderr)
    ],
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - PID_%(process)d - %(message)s'
)

# Create a logger
logger = logging.getLogger(__name__)
