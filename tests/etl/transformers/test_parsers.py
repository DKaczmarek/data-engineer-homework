import pytest
from datetime import datetime

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T
from pyspark.sql import functions as F

from homework.etl.transformers import parsers


@pytest.fixture
def session() -> SparkSession:
    return SparkSession.builder.master("local").getOrCreate()


def test_tokenize_raw_events(session: SparkSession):
    example_data = session.createDataFrame(
        pd.DataFrame(
            [{"rawCol": "http://test.com/site|lang|<000>123|id|/param?d=11"}]
        )
    )
    expected_output = [
        "http://test.com/site", "lang", "<000>123", "id", "/param?d=11"
    ]
    tested_parsers = parsers.tokenize_raw_events("rawCol")
    actual_output = (
        example_data.withColumn("tokens", tested_parsers)
        .select("tokens")
        .head()[0]
    )

    assert expected_output == actual_output


def test_parse_parameters_from_tokens(session: SparkSession):
    example_data = session.createDataFrame(
        pd.DataFrame([
            {"tokenCol": ["a", "b", "c", "d", "/p?d=11"]},
            {"tokenCol": ["a", "b", "c", "d", "/p?d=11&a=1"]},
            {"tokenCol": ["a", "b", "c", "d", "/p?d=11&a=1&g=10"]},
            {"tokenCol": ["a", "b", "c", "d", ""]},
            {"tokenCol": ["a", "b", "c", "d"]}
        ])
    )
    expected_output = [
        ["/p", "d=11"],
        ["/p", "d=11", "a=1"],
        ["/p", "d=11", "a=1", "g=10"],
        [""],
        None
    ]
    tested_parsers = parsers.parse_parameters_from_tokens("tokenCol")
    actual_output = (
        example_data
        .withColumn("params", tested_parsers)
        .select("params").head(5)
    )
    actual_output = [el[0] for el in actual_output]

    assert expected_output == actual_output


def test_timestamp_from_tokens(session: SparkSession):
    example_data = session.createDataFrame(
        pd.DataFrame([
            {"tokenCol": ["a", "b", "<134>2010-02-02T12:22:22Z"]},
            {"tokenCol": ["a", "b", "<134>2050-02-02T00:59:10Z"]},
            {"tokenCol": ["a", "b", "<134>2010-02-02T25:22:22Z"]},
            {"tokenCol": ["a", "b", ""]},
            {"tokenCol": ["a", "b", None]}
        ])
    )
    expected_output = [
        datetime(2010, 2, 2, 12, 22, 22),
        datetime(2050, 2, 2, 0, 59, 10),
        None, None, None
    ]
    tested_parsers = parsers.parse_timestamp_from_tokens("tokenCol")
    actual_output = (
        example_data
        .withColumn("parsed", tested_parsers)
        .select("parsed").head(5)
    )
    actual_output = [el[0] for el in actual_output]

    assert expected_output == actual_output


def test_parse_user_id_from_tokens(session: SparkSession):
    example_data = session.createDataFrame(
        pd.DataFrame([
            {"tokenCol": ["a", "b", "c", "a12"]},
            {"tokenCol": ["a", "b", "c", ""]},
            {"tokenCol": ["a", "b", "c"]}
        ])
    )
    expected_output = [
        "a12", "", None
    ]
    tested_parsers = parsers.parse_timestamp_from_tokens("tokenCol")
    actual_output = (
        example_data
        .withColumn("parsed", parsers.parse_user_id_from_tokens("tokenCol"))
        .select("parsed").head(5)
    )
    actual_output = [el[0] for el in actual_output]

    assert expected_output == actual_output


def test_parse_param_value(session: SparkSession):
    example_data = session.createDataFrame(
        pd.DataFrame([
            {"paramCol": ["/p", "d=11", "a=1", "g=1a0"]},
            {"paramCol": ["/p", "d=12", "a=2"]},
            {"paramCol": ["/p", "d=13"]},
            {"paramCol": [""]},
            {"paramCol": []}
        ])
    )
    expected_output = ["11", "12", "13", None, None]
    tested_parsers = parsers.parse_param_value("d", "paramCol")
    actual_output = (
        example_data
        .withColumn("parsed", tested_parsers)
        .select("parsed").head(5)
    )
    actual_output = [el[0] for el in actual_output]

    assert expected_output == actual_output

    expected_output = ["1", "2", None, None, None]
    tested_parsers = parsers.parse_param_value("a", "paramCol")
    actual_output = (
        example_data
        .withColumn("parsed", tested_parsers)
        .select("parsed").head(5)
    )
    actual_output = [el[0] for el in actual_output]

    assert expected_output == actual_output

    expected_output = [None, None, None, None, None]
    tested_parsers = parsers.parse_param_value("x", "paramCol")
    actual_output = (
        example_data
        .withColumn("parsed", tested_parsers)
        .select("parsed").head(5)
    )
    actual_output = [el[0] for el in actual_output]

    assert expected_output == actual_output
