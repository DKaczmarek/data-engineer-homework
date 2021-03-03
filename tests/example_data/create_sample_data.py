import random


from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as st


output_path = "data1.csv"

spark = SparkSession.builder.master("local").getOrCreate()
generator = random.Random()
generator.seed(2077)

schema = st.StructType(
    [
        st.StructField("user", st.StringType(), True),
        st.StructField("value", st.IntegerType(), True),
        st.StructField("time", st.IntegerType(), True),
    ]
)

data = [
    (
        generator.choice(["a", "b", "c", "d"]),
        generator.randint(0, 100),
        generator.randint(0, 1000),
    )
    for _ in range(0, 100)
]

dataframe = spark.createDataFrame(data, schema)
dataframe.write.mode("overwrite").csv(output_path, header=True)
