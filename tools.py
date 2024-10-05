# tools.py

import logging
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler
import paramiko.util
from pathlib import Path

import config


def create_directories(paths):
    try:
        for path in paths:
            Path(path).mkdir(parents=True, exist_ok=True)

    except Exception as e:
        logging.critical(str(e))


def setup_logging_queue_processor(log_queue, log_name):
    log_file_name = f'{config.PATHS["LOG_PATH"]}/{log_name}'
    log_handler = TimedRotatingFileHandler(log_file_name, when='midnight', backupCount=10)
    logging.basicConfig(level=logging.INFO, format=config.LOG_FORMAT, handlers=[log_handler])
    logging.info("Log processing process started")

    while True:
        try:
            if not log_queue.empty():
                message, log_level = log_queue.get(timeout=1)
                logging.log(log_level, message)  # Log message with level

        except Exception as e:
            logging.error(f"Error processing log message: {str(e)}")


def get_recent_dates(period_in_days):
    return [datetime.strftime(datetime.today() - timedelta(days=i), '%Y%m%d')
            for i in range(period_in_days)
            ][::-1]
