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
        return (
            input_data.withColumn(
                "tokens", parsers.tokenize_raw_events(raw_column="raw")
            )
            .select(
                parsers.parse_parameters_from_tokens().alias("parameters"),
                parsers.parse_user_id_from_tokens().alias("user_id"),
                parsers.parse_timestamp_from_tokens().alias("timestamp"),
            )
            .select(
                "user_id",
                "timestamp",
                parsers.parse_param_value(key="a").alias("article_id"),
                parsers.parse_param_value(key="c").alias("wiki_id"),
            )
        )

    def filter_events(self, input_data: DataFrame) -> DataFrame:
        self._log_info("filtering events")
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
        self._log_info("creating users profiles")
        data = self.find_edge_events(input_data)
        data = self.reduce_users_events(data)
        is_same_wiki = F.col("first_event").getItem(1) == F.col("last_event").getItem(1)
        is_same_article = (
            F.col("first_event").getItem(0) == F.col("last_event").getItem(0)
        ) & is_same_wiki
        return data.withColumn(
            "is_same_wiki", F.when(is_same_wiki, True).otherwise(False)
        ).withColumn("is_same_article", F.when(is_same_article, True).otherwise(False))

    def find_edge_events(
        self, input_data: DataFrame, user_column: str = "user_id"
    ) -> DataFrame:
        user_window = (
            Window()
            .partitionBy(user_column)
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )
        return (
            input_data.orderBy("timestamp", ascending=True)
            .select(
                "*",
                F.array(F.col("article_id"), F.col("wiki_id")).alias(
                    "article_wiki_ids"
                ),
            )
            .select(
                "user_id",
                F.first("article_wiki_ids").over(user_window).alias("first_event"),
                F.last("article_wiki_ids").over(user_window).alias("last_event"),
            )
        )

    def reduce_users_events(self, input_data: DataFrame) -> DataFrame:
        return input_data.drop_duplicates(["user_id", "first_event", "last_event"])

    def finalize(self, input_data: DataFrame) -> DataFrame:
        self._log_info("finalize data")
        return input_data.select("user_id", "is_same_article", "is_same_wiki").orderBy(
            "user_id", ascending=True
        )

    def _log_info(self, msg: str):
        if ModuleLogger.logger:
            ModuleLogger.info(msg)
        else:
            print(msg)
