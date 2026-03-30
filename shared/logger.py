"""
Logging module for BookedUp agent system.
Each agent gets its own log file in ~/bookedup/logs/.
"""

import logging
import os
from datetime import datetime

LOGS_DIR = "/Users/bookedup/bookedup/logs"


def get_logger(agent_name):
    logger = logging.getLogger(f"bookedup.{agent_name}")

    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    log_file = os.path.join(LOGS_DIR, f"{agent_name}.log")
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
