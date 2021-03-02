from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType


def count_user_events(user_column: str = "user_id"):
    user_window = Window().partitionBy(user_column)
    return F.count("*").over(user_window)


def get_first_user_event_value(target_column: str, user_column: str = "user_id"):
    return F.first(F.col(target_column)).over(
        Window()
        .partitionBy(user_column)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )


def get_last_user_event_value(target_column: str, user_column: str = "user_id"):
    return F.last(F.col(target_column)).over(
        Window()
        .partitionBy(user_column)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
