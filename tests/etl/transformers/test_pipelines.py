import pytest
from datetime import datetime

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T
from pyspark.sql import functions as F

from homework.etl.transformers import pipelines


@pytest.fixture
def session() -> SparkSession:
    return SparkSession.builder.master("local").getOrCreate()


def test_ArticleWikisVisitTransformer(session: SparkSession):
    example_data = session.createDataFrame(
        pd.DataFrame(
            [
                {"raw": "test.com/site|lang|<134>2010-01-01T12:00:00Z|id1|/param?a=11&c=1"},
                {"raw": "test.com/site|lang|<134>2010-01-01T12:00:00Z|id2|/param?a=11&c=1"},
                {"raw": "test.com/site|lang|<134>2010-01-01T11:00:00Z|id2|/param?a=11&c=2"},
                {"raw": "test.com/site|lang|<134>2010-01-01T14:00:00Z|id3|/param?a=11&c=1"},
                {"raw": "test.com/site|lang|<134>2010-01-01T14:00:00Z|id1|/param?a=12&c=1"},
                {"raw": "test.com/site|lang|<134>2010-01-01T10:00:00Z|id4|/param?a=1&c=1"},
                {"raw": "test.com/site|lang|<134>2010-01-01T11:00:00Z|id4|/param?a=22&c=1"},
                {"raw": "test.com/site|lang|<134>2010-01-01T14:00:00Z|id4|/param?a=1&c=1"},
            ]
        )
    )

    expected_output = [
        ["id1", False, True],
        ["id2", False, False],
        ["id4", True, True]
    ]

    actual_output = (
        pipelines
        .ArticleWikisVisitTransformer()
        .transform(example_data)
        .head(5)
    )
    actual_output = [
        list(el.asDict().values())
        for el in actual_output
    ]

    assert expected_output == actual_output
