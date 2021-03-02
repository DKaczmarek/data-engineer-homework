from pyspark.sql import SparkSession


def init_spark_session(arguments):
    if arguments.local:
        return (
            SparkSession.builder.appName("homework-data-transform")
            .master("local[4]")
            .getOrCreate()
        )
    else:
        return SparkSession.builder.getOrCreate()
