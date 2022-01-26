
import findspark
findspark.init("/home/guy/hadoop/spark-3.2.0-bin-hadoop3.2")
from pyspark.sql import SparkSession
from kafka import KafkaConsumer
import json
import time
from os.path import abspath

warehouse_location = abspath('/user/hive/warehouse/testdb')
bootstrapServers = "****:9092"
topics = 'test' 
brokers = ['****:9092']
consumer = KafkaConsumer(topics)

spark = SparkSession \
    .builder \
    .appName("writeToHive") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

for message in consumer:
    events = json.loads(message.value)
    timeStamp=int(time.time())
    text = f'insert into testdb.table1 partition (size) values ({timeStamp}, "{events}")'
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.sql(text)
    spark.sql("SELECT count(*) FROM testdb.table1").show()


