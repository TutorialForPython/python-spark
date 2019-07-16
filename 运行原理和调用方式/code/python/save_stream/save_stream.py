import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import window
spark = SparkSession.builder.appName("SaveStreamApp").getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "topic1") \
  .load()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
is_stream = df.isStreaming

def foreach_batch_function(df,epoch_id):
    timestp = int(time.time())
    df.groupBy(df.key).count().write.parquet(f"output/{timestp}_{epoch_id}.parquet")


df.writeStream.trigger(processingTime='120 seconds').foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()
