import org.apache.spark.sql.SparkSession

object HelloWorldApp {
  def main(args: Array[String]) {
    val logFile = "/usr/local/Cellar/apache-spark/2.4.0/README.md"
    val spark = SparkSession.builder.appName("Hello World").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}