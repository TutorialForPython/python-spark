from operator import add
from pyspark.sql import SparkSession
from redis import StrictRedis

logFile = "/usr/local/Cellar/apache-spark/2.4.0/README.md"  # Should be some file on your system
redis = StrictRedis.from_url('redis://@localhost:6379/1')
spark = SparkSession.builder.appName("wordcloudApp").getOrCreate()
lines= spark.read.text(logFile).cache().rdd.map(lambda r: r[0])

counts = lines.flatMap(lambda x: x.split(' ')) \
    .map(lambda x: (x, 1)) \
    .reduceByKey(add)
output = counts.collect()
redis.flushdb()
for (word, count) in output:
    redis.set(word,count)
spark.stop()