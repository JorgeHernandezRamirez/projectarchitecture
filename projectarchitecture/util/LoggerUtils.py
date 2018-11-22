import logging

import os

class LoggerUtils:

    @staticmethod
    def initialize():
        logging.basicConfig(format='%(asctime)-15s %(levelname)s %(message)s')

    @staticmethod
    def get_logger(loggername):
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)-15s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        logger = logging.getLogger(loggername)
        logger.setLevel(os.environ.get("LOGLEVEL", "INFO"))
        logger.addHandler(handler)
        return logger
