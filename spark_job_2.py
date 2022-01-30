

import findspark
from pandas import to_datetime

findspark.init("/home/guy/hadoop/spark-3.2.0-bin-hadoop3.2")
from os.path import abspath
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, desc, lit, to_date, from_unixtime
from pyspark.sql.types import DateType
import time
from datetime import datetime

warehouse_location = abspath('/user/hive/warehouse/testdb')

spark = SparkSession \
    .builder \
    .appName("pyspark_hive") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

sqlContext = SQLContext(spark)

print('1st table')
df1 = spark.sql("SELECT * FROM testdb.table4") #.show()
# df1.show()
# save / write the DataFrame to parquet file format in HDFS
df1.repartition(1).write.parquet(f'hdfs://localhost:9000/user/guy/originalUnixTime.{int(time.time())}')



# df1.show()
# df1.printSchema()
# spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
# print(f' ===> numner of items in the table {df1.count()}')
# print(f' ===> number of [BIG] in the table {df1.where(df1["size"] == "big").count()}')


# copying the data from table1 to table2
# spark.sql("INSERT INTO testdb.table2  table  testdb.table4 ;")

# print('2nd table')
# selecting from table 2 only the items of "BIG" into data frame
# df2 = spark.sql("SELECT * FROM testdb.table4 where size = 'big'") #.show()
# df2.show()

# copying the data from hive table to parquet files format in another place in HDFS
# map reduced paralalized job
# columns = df1.columns
# print('save parquet to local user folder')
# df2a = sqlContext.createDataFrame(df1.rdd.map(lambda a: a),columns)
# df2a.write.mode('overwrite').parquet(f'hdfs://localhost:9000/user/guy/{int(time.time())}')

# running rdd paralalized job to select items from data frame
# print('print the size of data')
# counter = 0 
# stringsDS = df1.rdd.map(lambda row: "Key: %s, Value: %d" % (row.size, row.tm_stamp))
# for record in stringsDS.collect():
#     counter = counter +1
# print(counter)

df1.groupBy("size").count().orderBy(desc("count")).take(4)

df1.filter(df1.size == 'small').show()

df2 = df1.withColumn("DateTime", from_unixtime('tm_stamp').cast(DateType()))
df2.take(4)

df2.repartition(1).write.parquet(f'hdfs://localhost:9000/user/guy/original_unixTime_daTime.{int(time.time())}')


