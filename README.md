# Data Engineer Homework

The Data Engineer homework is solution for automatic preprocessing of *homework* log data. The implementation is based on Apache Spark environment with use of python API. The app is capable of performing:

- Input data reading and tokenizing (Text files)
- Basic filtering
- Transforming data to required form

## Getting Started

### Prerequisites

To run the project on local machine, Python programming language will be required along with **Apache Spark** installed with all dependencies it requires to work.

Project environment:

- Ubuntu 18.04
- Apache Spark 2.4.7
- \>= Python 3.6.13

In case, you're insterested in running app by `spark-submit` only, then you can skip this step.

## Install & Run

The project can be set up in two ways depending on target execution environment.

### Local

It's recommended for developing or testing. Project can be installed as package using `setup.py` file. Simply enter inside project directory:

```bash
python setup.py install
```

In case you are using **poetry** as your dependency manager, than you can install package by:

```bash
poetry install
```

After installing, the app can be run from command line where it is accessed by `homework` command. Inteface is rather simply:

```bash
usage: homework [-h] -i INPUT_PATH -o OUTPUT_PATH [-l]

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT_PATH, --input-path INPUT_PATH
                        An input file path
  -o OUTPUT_PATH, --output-path OUTPUT_PATH
                        An output file path
  -l, --local           Indicator if passed job is run in local mode (session will be created), else session is run in remote mode (existing session will be used)
```

Example:

```bash
# run in local mode
homework -i data/data_sample.log -o data/data_output.csv --local
```

### Remote

It's recommended for running project on spark cluster via `spark-submit` command. In order to run project this way, the project source code is has to be packed along with all external dependencies.

It can be achieved by commands:

```bash
 #!/bin/bash  
JOBDIR=./job
mkdir $JOBDIR
zip -r $JOBDIR/homework.zip homework \
    -x /*__pycache__/* \
    -x homework/main.py;
cp homework/main.py $JOBDIR/main.py
```

These commands should create a new directory `job` with structure:

```bash
job
├── homework.zip    # zipped source code
└── main.py         # executable file
```

These files should be sent to place where can be accessed by instance responsible for `spark-submit` execution like for example **Apache Airflow** dag directory.

The application shares the same interface regardless of running method, but in of `spark-submit` method the command has to be enriched with additional spark-related parameters.

Example (remote yarn cluster):

```bash
spark-submit --master yarn \
             --deploy-mode cluster \
             --conf "spark.executor.memory=8G" \
             #... 
             --conf "spark.driver.memory=8G" \
             --py-files ./job/homework.zip \
             ./job/main.py \
             -i data/data_sample.log \
             -o data/data_output.csv
```

When running app by `spark-submit` keep in mind to pass **homework.zip** by `py-files` argument. Due to this, pyfiles will be shared between spark executors and added to `PYTHON_PATH` automatically.


## Contact

- Dominik Kaczmarek \<dominik.tomasz.kaczmarek@gmail.com\>