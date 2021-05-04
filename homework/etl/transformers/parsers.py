from pyspark.sql import Column
from pyspark.sql import functions as F


def tokenize_raw_events(raw_column: str = "raw") -> Column:
    return F.split(F.col(raw_column), "\|")


def parse_parameters_from_tokens(tokens_column: str = "tokens") -> Column:
    sep_pattern = "\&|\?"
    return F.split(F.col(tokens_column).getItem(4), sep_pattern)


def parse_timestamp_from_tokens(tokens_column: str = "tokens") -> Column:
    timeformat = "'<134>'yyyy-MM-dd'T'HH:mm:ss'Z'"
    return F.to_timestamp(F.col(tokens_column).getItem(2), timeformat)


def parse_user_id_from_tokens(tokens_column: str = "tokens") -> Column:
    return F.col(tokens_column).getItem(3)


def parse_param_value(key: str, params_column: str = "parameters") -> Column:
    return F.split(
        F.expr(f"filter({params_column}, el -> el rlike '^{key}=.*')").getItem(0),
        "="
    ).getItem(1)
