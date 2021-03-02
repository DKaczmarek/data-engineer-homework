from abc import ABC, abstractmethod

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

from homework.common.config import UserConfiguration
from homework.etl.loaders import TextLoader
from homework.etl.transformers import ArticleWikisVisitTransformer
from homework.etl.writers import CSVWriter


class ETLDriver:
    def __init__(self, spark: SparkSession):
        self.loader = TextLoader(spark)
        self.transformer = ArticleWikisVisitTransformer()
        self.writer = CSVWriter()

    def run(self):
        data = self.loader.load()
        data = self.transformer.transform(data)
        self.writer.save(data)
