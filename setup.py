# -*- coding: utf-8 -*-
from setuptools import setup

readme = ''

packages = \
['homework', 'homework.common', 'homework.etl', 'homework.etl.transformers']

package_data = \
{'': ['*']}

install_requires = \
['pyspark==2.4.7']

entry_points = \
{'console_scripts': ['homework = homework.main:run']}

keywords = \
["homework", "data", "engineering", "spark", "pyspark"]

extras_require = \
{"dev": ["pytest==6.2.2", "flake8==3.8.4", "black==20.*,>=20.8.0.b1", "pandas==0.25.3"]}

setup_kwargs = {
    'name': 'homework',
    'version': '0.1.0',
    'description': 'The tool to automate homework data transforming',
    'long_description': readme,
    'author': 'Dominik Kaczmarek',
    'author_email': 'dominik.tomasz.kaczmarek@gmail.com',
    'maintainer': None,
    'maintainer_email': None,
    'url': None,
    'packages': packages,
    'package_data': package_data,
    'install_requires': install_requires,
    'extras_reqiure': extras_require,
    'entry_points': entry_points,
    'python_requires': '>=3.6,<4.0',
}


setup(**setup_kwargs)
