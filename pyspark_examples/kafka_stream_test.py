# Subscribe to 1 topic
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("stream").getOrCreate()
dsraw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "queueing.transactions").load()
ds = dsraw.selectExpr("CAST(key AS STRING)")
print(type(dsraw))
print(type(ds))



rawQuery = dsraw \
        .writeStream \
        .queryName("qraw")\
        .format("memory")\
        .start()

raw = spark.sql("select * from qraw")
raw.show()
