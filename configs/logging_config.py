from logging.handlers import RotatingFileHandler
import logging
import os
import gzip
import shutil
from dotenv import load_dotenv
load_dotenv()
root_dir = os.environ['PROJECT_ROOT']

DB_WRITE = 25
logging.addLevelName(DB_WRITE, 'DB_WRITE')
def db_write(self, message, *args, **kwargs):
    if self.isEnabledFor(DB_WRITE):
        self._log(DB_WRITE, message, args, **kwargs)
logging.Logger.db_write = db_write

def gz_rotator(source, dest):
    with open(source, 'rb') as f_in:
        with gzip.open(f"{dest}.gz", 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(source)

COLORS = {
    'DEBUG':    '\033[36m',
    'INFO':     '\033[32m',
    'DB_WRITE': '\033[34;1m',
    'WARNING':  '\033[33m',
    'ERROR':    '\033[31m',
    'CRITICAL': '\033[35m',
    'RESET':    '\033[0m'
}

class ColorFormatter(logging.Formatter):
    def format(self, record):
        color = COLORS.get(record.levelname, COLORS['RESET'])
        record.levelname = f"{color}{record.levelname}{COLORS['RESET']}"
        return super().format(record)

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    terminal_handler = logging.StreamHandler()
    terminal_handler.setFormatter(ColorFormatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    terminal_handler.setLevel(logging.INFO)

    file_handler = RotatingFileHandler(
        filename=f"{root_dir}/logs/pipeline.log",
        maxBytes=500_000_000,
        backupCount=10
    )
    file_handler.rotator = gz_rotator
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    file_handler_debug = RotatingFileHandler(
        filename=f"{root_dir}/logs/pipeline_debug.log",
        maxBytes=1_000_000_000,
        backupCount=10
    )
    file_handler_debug.rotator = gz_rotator
    file_handler_debug.setFormatter(formatter)
    file_handler_debug.setLevel(logging.DEBUG)

    logger.addHandler(terminal_handler)
    logger.addHandler(file_handler)
    logger.addHandler(file_handler_debug)