from typing import List


class UserConfiguration:

    input_path = None
    output_path = None


def init_configuration(arguments):

    UserConfiguration.input_path = arguments.input_path
    UserConfiguration.output_path = arguments.output_path
