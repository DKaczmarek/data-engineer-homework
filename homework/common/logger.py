import logging
from datetime import datetime

from pyspark.sql import SparkSession


class ModuleLogger:
    logger = None

    @classmethod
    def init(cls):
        log_format = "%(asctime)s %(levelname)s %(name)s: %(message)s"
        cls.logger = logging.getLogger("homework")
        cls.logger.setLevel(logging.DEBUG)
        stream_handler = logging.StreamHandler()
        formatter = logging.Formatter(log_format)
        stream_handler.setFormatter(formatter)
        cls.logger.addHandler(stream_handler)
        cls.info("logger is initialized")

    @classmethod
    def info(cls, msg: str):
        cls.logger.info(msg)
        return None

    @classmethod
    def warn(cls, msg: str):
        cls.logger.warn(msg)
        return None

    @classmethod
    def debug(cls, msg: str):
        cls.logger.debug(msg)
        return None
