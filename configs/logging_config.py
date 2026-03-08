from logging.handlers import RotatingFileHandler
import logging
import os
from dotenv import load_dotenv

load_dotenv()
root_dir = os.environ['PROJECT_ROOT']

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    terminal_handler = logging.StreamHandler()
    terminal_handler.setFormatter(formatter)
    terminal_handler.setLevel(logging.INFO)

    file_handler = RotatingFileHandler(
        filename = f"{root_dir}/logs/pipeline.log",
        maxBytes= 10_000_000,
        backupCount = 5
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    file_handler_debug = RotatingFileHandler(
        filename = f"{root_dir}/logs/pipeline_debug.log",
        maxBytes= 20_000_000,
        backupCount= 5
    )

    file_handler_debug.setFormatter(formatter)
    file_handler_debug.setLevel(logging.DEBUG)

    logger.addHandler(terminal_handler)
    logger.addHandler(file_handler) 
    logger.addHandler(file_handler_debug)