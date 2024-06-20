import time

import numpy as np
from pyspark import SparkConf
from pyspark.sql import SparkSession

start = time.time()
conf = SparkConf()
conf.set("spark.eventLog.enabled", "true")

spark = SparkSession.builder.appName("Skewed").config(conf=conf).getOrCreate()

print("Generating")
data_size = 5000000
skewedData = np.column_stack((np.random.choice([1, 2, 3, 4, 5], size=data_size, p=[0.7, 0.1, 0.1, 0.05, 0.05]), np.random.rand(data_size)))
other_data = [[i, np.random.rand()] for i in range(1, 6)]

print("Loading")
df1 = spark.createDataFrame(skewedData, ["key", "value1"])
df2 = spark.createDataFrame(other_data, ["key", "value2"])

print("Performing")
joined_df = df1.join(df2, "key")
joined_df.explain()

partitions = joined_df.rdd.glom().map(len).collect()
print(f"Nombre de lignes dans chaque partition : {partitions}")

count = joined_df.count()
print(f"Nombre de lignes après la jointure : {count}")

spark.stop()

print(f"Time computing: {time.time() - start}")
