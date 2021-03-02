from abc import ABC, abstractmethod

from pyspark.sql import SparkSession, DataFrame

from homework.common.config import UserConfiguration
from homework.common.logger import ModuleLogger


class DataLoader(ABC):
    @abstractmethod
    def load(self) -> DataFrame:
        """Implementation of loading method"""
        raise NotImplementedError()


class TextLoader(DataLoader):
    def __init__(self, spark: SparkSession):
        self.session = spark

    def load(self) -> DataFrame:
        ModuleLogger.info("data read from:")
        ModuleLogger.info(UserConfiguration.input_path)
        return self.session.read.text(
            UserConfiguration.input_path, lineSep=None
        ).withColumnRenamed("value", "raw")
