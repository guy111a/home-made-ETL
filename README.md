# home-made-ETL


in order not to forget and have some experience .. even if only as a game.

so i installed the following servers on my wsl ( linux in windows sub-system )

hadoop ( hdfs ) - a distributed file system
zookeeper - high performance distributed coordination server
hive - a data warehouse
kafka - a distributed event streaming platform
spark - an engine for large-scale data processing
using pytho3 and a bit of scala, i created the following ETL

data generator, python script that is randomly selecting a size [small, medium, big, no-size] and sending the size to a kafka topic
kafka is moving the data in the topic
data consumer, python script that is listening to the kafka topic, accepting the data, adding the current time stamp of the event and pushing the combined information into the hive database
hive, in a partitioned table is storing the information according to the correct partition ( based upon the size )
anther python script is reading the data that is stored in the hive table, and saving the information that is newer since last time in another location in HDFS in parquet files.
lastly, a spark-submit job is running to manipulate / enhance the information in the hive tables. showing several interesting things in the data that is collected.
