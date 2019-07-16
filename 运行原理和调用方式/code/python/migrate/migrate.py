from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MigrateApp").config("spark.driver.extraClassPath", "/Users/huangsizhe/Workspace/Documents/TutorialForSpark/spark-dev-env/code/migrate/libs/postgresql-42.2.5.jar").getOrCreate()
pgdf = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql:test") \
    .option("dbtable", "users") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .load()
pgdf.write.parquet("output/users.parquet")
spark.stop()
