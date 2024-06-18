"""
Contains basic functions to initiate session on HDFS and initiate a log 
to save errors
"""

import logging


def logging_creation(logging_file="credentials.logging"):
    """
    Use: Autentificate the user for the HDFS virtual machine
    """
    with open(logging_file) as f:
        key_values = f.read().split()

        logging_info = {}
        while len(key_values) != 0:
            logging_info[key_values.pop(0).split(":")[0]] = key_values.pop(1)

    return logging_info


def log_config(log_name):
    """
    Use: Create and configure a log mechanism to save errors
    occurring during the process
    """
    logging.basicConfig(
        filename=log_name, format="%(asctime)s %(message)s", filemode="w"
    )

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    return logger
