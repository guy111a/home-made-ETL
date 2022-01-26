


from os.path import abspath
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col
import time

import findspark
findspark.init("/home/guy/hadoop/spark-3.2.0-bin-hadoop3.2")

warehouse_location = abspath('/user/hive/warehouse/testdb')

spark = SparkSession \
    .builder \
    .appName("pyspark_hive") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

sqlContext = SQLContext(spark)

print('1st table')
df1 = spark.sql("SELECT * FROM testdb.table1") #.show()
# df1.show()
df1.printSchema()
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
print(f' ===> numner of items in the table {df1.count()}')
print(f' ===> number of [BIG] in the table {df1.where(df1["size"] == "big").count()}')


# copying the data from table1 to table2
spark.sql("INSERT INTO testdb.table2  table  testdb.table1 ;")

print('2nd table')
# selecting from table 2 only the items of "BIG" into data frame
df2 = spark.sql("SELECT * FROM testdb.table2 where size = 'big'") #.show()
df2.show()

# copying the data from hive table to parquet files format in another place in HDFS
# map reduced paralalized job
columns = df2.columns
print('save parquet to local user folder')
df2a = sqlContext.createDataFrame(df2.rdd.map(lambda a: a),columns)
df2a.write.mode('overwrite').parquet(f'hdfs://localhost:9000/user/guy/{int(time.time())}')



# running rdd paralalized job to select items from data frame
print('printthe size of data')
counter = 0 
stringsDS = df1.rdd.map(lambda row: "Key: %s, Value: %d" % (row.size, row.tm_stamp))
for record in stringsDS.collect():
    counter = counter +1
print(counter)