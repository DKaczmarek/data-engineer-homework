from abc import ABC, abstractmethod

from pyspark.sql import SparkSession, DataFrame

from homework.common.config import UserConfiguration
from homework.common.logger import ModuleLogger


class DataWriter(ABC):
    @abstractmethod
    def save(self, data: DataFrame):
        """Implementation of saving method"""
        raise NotImplementedError()


class CSVWriter(DataWriter):
    def __init__(self, partitions: int = 50):
        super(DataWriter, self).__init__()
        self.partitions = partitions

    def save(self, data: DataFrame):
        ModuleLogger.info("data saved at:")
        ModuleLogger.info(UserConfiguration.output_path)
        return data.coalesce(self.partitions).write.csv(
            UserConfiguration.output_path, header=True, mode="overwrite", sep=","
        )
