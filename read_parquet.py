

import findspark
findspark.init("/home/guy/hadoop/spark-3.2.0-bin-hadoop3.2")
from os.path import abspath
from pyspark.sql import SparkSession

appName = "Scala Parquet Example"
master = "local"

spark = SparkSession.builder.appName(appName).master(master).getOrCreate()

df2 = spark.read.parquet("/user/guy/1643577716.parquet/part-00000-0560dc5d-501b-4630-b8ee-1816b889249e-c000.snappy.parquet")
df2.show()
