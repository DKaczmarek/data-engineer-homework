from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from homework.etl.transformers import parsers
from homework.etl.transformers import functions as func
from homework.common.logger import ModuleLogger


class DataTransformer(ABC):
    @abstractmethod
    def transform(self, data: DataFrame) -> DataFrame:
        """Implementation of transforming method"""
        raise NotImplementedError()


class ArticleWikisVisitTransformer(DataTransformer):
    def transform(self, data: DataFrame) -> DataFrame:
        data = self.parse_raw_events(data)
        data = self.filter_events(data)
        data = self.create_users_profiles(data)
        data = self.finalize(data)
        return data

    def parse_raw_events(self, input_data: DataFrame) -> DataFrame:
        ModuleLogger.info("parsing raw events")
        return (
            input_data.withColumn("tokens", parsers.tokenize_raw_events())
            .withColumn("parameters", parsers.parse_parameters_from_tokens())
            .withColumn("user_id", parsers.parse_user_id_from_tokens())
            .withColumn("article_id", parsers.parse_param_value(key="a"))
            .withColumn("wiki_id", parsers.parse_param_value(key="c"))
            .withColumn("timestamp", parsers.parse_timestamp_from_tokens())
            .drop("raw", "tokens", "parameters")
        )

    def filter_events(self, input_data: DataFrame) -> DataFrame:
        ModuleLogger.info("filtering events")
        user_window = Window().partitionBy("user_id")
        article_not_null = F.col("article_id").isNotNull()
        wiki_not_null = F.col("wiki_id").isNotNull()
        more_than_one_visit = F.col("visits") > 1
        return (
            input_data.where(article_not_null & wiki_not_null)
            .withColumn("visits", func.count_user_events())
            .where(more_than_one_visit)
            .drop("visits")
        )

    def create_users_profiles(self, input_data: DataFrame) -> DataFrame:
        ModuleLogger.info("creating users profiles")
        data = self.find_edge_events(input_data)
        data = self.reduce_users_events(data)
        is_same_article = F.col("first_article") == F.col("last_article")
        is_same_wiki = F.col("first_wiki") == F.col("last_wiki")
        return (
            data
            .withColumn("is_same_article", F.when(is_same_article, True).otherwise(False))
            .withColumn("is_same_wiki", F.when(is_same_wiki, True).otherwise(False))
        )

    def find_edge_events(self, input_data: DataFrame) -> DataFrame:
        return (
            input_data.orderBy("timestamp", ascending=True)
            .withColumn("first_article", func.get_first_user_event_value("article_id"))
            .withColumn("last_article", func.get_last_user_event_value("article_id"))
            .withColumn("first_wiki", func.get_first_user_event_value("wiki_id"))
            .withColumn("last_wiki", func.get_last_user_event_value("wiki_id"))
        )

    def reduce_users_events(self, input_data: DataFrame) -> DataFrame:
        return input_data.drop_duplicates(
            ["user_id", "first_article", "last_article", "first_wiki", "last_wiki"]
        )

    def finalize(self, input_data: DataFrame) -> DataFrame:
        ModuleLogger.info("finalize data")
        return input_data.select(
            "user_id", "is_same_article", "is_same_wiki"
        ).orderBy("user_id", ascending=True)