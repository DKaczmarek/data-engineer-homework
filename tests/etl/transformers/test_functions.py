import pytest
import random

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T
from pyspark.sql import functions as F

from homework.etl.transformers import functions


@pytest.fixture
def session() -> SparkSession:
    return SparkSession.builder.master("local").getOrCreate()


@pytest.fixture
def example_data(session: SparkSession) -> DataFrame:
    sample_path = "tests/example_data/data1.csv"
    schema = T.StructType(
        [
            T.StructField("user", T.StringType(), True),
            T.StructField("value", T.IntegerType(), True),
            T.StructField("time", T.IntegerType(), True),
        ]
    )
    return session.read.csv(sample_path, header=True, schema=schema)


def test_count_user_events(example_data: DataFrame):
    expected_output = [21, 31, 24, 24]
    tested_function = functions.count_user_events("user")

    actual_output = (
        example_data.withColumn("count", tested_function)
        .select("user", "count")
        .distinct()
        .select("count")
        .rdd.map(lambda el: el[0])
        .collect()
    )

    assert sorted(expected_output) == sorted(actual_output)


def test_first_user_event_value(session: SparkSession, example_data: DataFrame):
    expected_output = [93, 71, 86, 26]
    tested_function = functions.get_first_user_event_value("value", "user")

    actual_output = (
        example_data.orderBy("time")
        .withColumn("first", tested_function)
        .select("user", "first")
        .distinct()
        .toPandas()["first"]
        .values
    )

    assert sorted(expected_output) == sorted(actual_output)


def test_last_user_event_value(session: SparkSession, example_data: DataFrame):
    expected_output = [11, 29, 71, 85]
    tested_function = functions.get_last_user_event_value("value", "user")

    actual_output = (
        example_data.orderBy("time")
        .withColumn("first", tested_function)
        .select("user", "first")
        .distinct()
        .toPandas()["first"]
        .values
    )

    assert sorted(expected_output) == sorted(actual_output)
