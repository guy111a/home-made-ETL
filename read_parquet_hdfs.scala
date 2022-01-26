
import spark.implicits._
val parquetFile = "/user/guy/1643202574/part-00006-aecadf97-26e8-490c-813c-b518c4435fd0-c000.snappy.parquet"

parquetFileDF.createOrReplaceTempView("parquetFile")
val namesDF = spark.sql("SELECT * FROM parquetFile limit 6")
namesDF.map(attributes => "timeStamp: " + attributes(0)).show()