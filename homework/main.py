import argparse

from homework.common.config import init_configuration
from homework.common.spark import init_spark_session
from homework.common.logger import ModuleLogger
from homework.etl.driver import ETLDriver


def parse_input_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i", "--input-path", type=str, required=True, help="An input file path"
    )
    parser.add_argument(
        "-o", "--output-path", type=str, required=True, help="An output file path"
    )
    parser.add_argument(
        "-l",
        "--local",
        action="store_true",
        required=False,
        help="""
            Indicator if passed job is run in local mode (session will be
            created), else session is run in remote mode (existing session
            will be used)
        """,
    )
    return parser.parse_args()


def run():

    arguments = parse_input_arguments()
    init_configuration(arguments)
    spark = init_spark_session(arguments)
    ModuleLogger.init()
    ETLDriver(spark).run()
    spark.stop()


if __name__ == "__main__":
    run()