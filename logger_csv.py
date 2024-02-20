import logging
import os
from datetime import datetime
import configparser

config = configparser.RawConfigParser()
config.read('config_csv.ini')


def configure_logger(logger_name, log_file_path):
    """
    Configure and return a logger.

    :param logger_name: Name of the logger.
    :param log_file_path: File path for the log file.
    :return: Configured logger.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_handler = logging.FileHandler(f"{log_file_path}_{current_date}.log")
    # %(asctime)s
    log_formatter = logging.Formatter('%(levelname)s %(message)s')
    log_handler.setFormatter(log_formatter)

    logger.addHandler(log_handler)
    return logger


# Log path from configuration
log_path = config['General']['LOG_PATH']

# Configuring a logger for the main module
main_logger = configure_logger(__name__, log_path)
