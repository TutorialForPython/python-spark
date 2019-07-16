# Spark的开发环境搭建


前一种方式没什么可以讲的,主要是第二种.

## helloworld

我们采用spark官网的例子,这个项目我们以scala作为编程语言.代码部分非常简单

+ helloworld/src/main/scala/helloworld.scala

```scala
import org.apache.spark.sql.SparkSession

object HelloWorldApp {
  def main(args: Array[String]) {
    val logFile = "$SPARK_HOME/README.md"
    val spark = SparkSession.builder.appName("Hello World").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}
```
+ helloworld/build.sbt

```sbt
name := "Hello World"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.1"
```

之后在项目根目录使用`sbt package`即可打包.

但实际我们发现会很慢,原因在于sbt在国内是被墙的.我们可以设置`~/.sbt/repositories`如下:

```
[repositories]
local
aliyun: http://maven.aliyun.com/nexus/content/groups/public/
typesafe: http://repo.typesafe.com/typesafe/ivy-releases/, [organization]/[module]/(scala_[scalaVersion]/)(sbt_[sbtVersion]/)[revision]/[type]s/[artifact](-[classifier]).[ext], bootOnly
sonatype-oss-releases
maven-central
sonatype-oss-snapshots
```

等待编译完成,会在项目的`target/scala-2.11`下看到`hello-world_2.11-1.0.jar`这就是我们要的app了.

我们可以使用入命令提交

```shell
spark-submit \
--class "HelloWorldApp" \
--master "local[4]" \
target/scala-2.11/hello-world_2.11-1.0.jar
```

scala的包多是java包,有第三方依赖可以在`https://mvnrepository.com/`查到如何安装配置,如果有第三方依赖,我们需要使用`--jars xxxx.jar,xxx.jar...`来导入.

```shell
spark-submit \
--class "HelloWorldApp" \
--master "local[4]" \
--jars xxxxx.jar,xxxxx.jar \
target/scala-2.11/hello-world_2.11-1.0.jar
```

## 任务管理

spark自带一个任务管理页面,单机版本不容易看出来,如果使用的是集群版本,我们可以通过访问`master`的`4040`端口(不是的话请联络管理员)看到任务的调度情况.

## 定时任务

spark并没有专门的定时任务管理工具,但我们可以利用[crontab](http://blog.hszofficial.site/introduce/2017/05/10/linux%E5%AE%9A%E6%97%B6%E4%BB%BB%E5%8A%A1%E5%B7%A5%E5%85%B7crontab/)来实现定时调用.

## 输入输出

spark只是一个计算框架,我们读取数据写入数据依然需要有额外的工具.上例中我们是从文件系统中读取,结果写入标准输出,当然真实场景比这个复杂的多,需要看我们的业务场景.比如我们可能是从hdfs/hive/hbase上读数据,处理好后写到redis中缓存,可能是从亚马逊的对象存储中读数据,处理好后更新进业务数据库,也可能是从消息队列比如kafka中读取消息,通过流处理再写入另一个消息队列比如rabbitmq.下面是比较常见的几种需求.

### 读写redis

这个例子是一个词云图,我们依然是读取文件,然后统计词频并将词频写入redis.

这个例子我们演示如何使用python写.

```python
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
```

执行
```shell
spark-submit \
--master "local[4]" \
wordcloud.py
```

spark接收`.py`文件或者由多个可执行文件放在一起打包成的zip文件.python的第三方依赖需要各个节点安装相同的依赖环境.

可以看出python写spark项目的优势--更低的复杂度,依赖管理也相对简单.

### 读写pg

这个例子模拟数据库的定时入库操作.我们从pg中读取数据,然后使用`PARQUET`格式写入到文件系统中.

这是一个业务中真实碰到过的需求,当时是使用一个定时任务将每天pg分好的前一天的表转成`PARQUET`格式写入亚马逊的对象存储.简化下这个例子我们定义一张User表

+ create_insert.sql

```sql
CREATE TABLE users
(
id INTEGER PRIMARY,
name TEXT,
age INTEGER
);
INSERT INTO users (name, age) VALUES ('hsz', 15);
INSERT INTO users (name, age) VALUES ('hzj', 16);
INSERT INTO users (name, age) VALUES ('zyf', 18);
INSERT INTO users (name, age) VALUES ('lyl', 20);
INSERT INTO users (name, age) VALUES ('yll', 15);
```

然后使用任务将数据导出

+ migrate.py

```python
from pyspark.sql import SparkSession
import psycopg2

spark = SparkSession.builder.appName("MigrateApp").config("spark.driver.extraClassPath", "/Users/huangsizhe/Workspace/Documents/TutorialForSpark/spark-dev-env/code/migrate/libs/postgresql-42.2.5.jar").getOrCreate()
pgdf = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql:test") \
    .option("dbtable", "users") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .load()
pgdf.filter(pgdf["age"] > 10).write.parquet("output/users.parquet")
spark.stop()
```

执行:

```shell
spark-submit \
--master "local[4]" \
--driver-class-path libs/postgresql-42.2.5.jar \
--jars libs/postgresql-42.2.5.jar \
migrate.py
```

注意我们使用了spark的`jdbc`,它需要指定数据库驱动的位置,需要分别在`SparkSession`中指定`spark.driver.extraClassPath`,启动时指定`--driver-class-path`和`--jars`pg的驱动一般是[postgresql](https://mvnrepository.com/artifact/org.postgresql/postgresql),mysql的驱动一般是[mysql-connector-java](https://mvnrepository.com/artifact/mysql/mysql-connector-java)

最终我们得到的也不是一个文件而是一个同名文件夹,不要紧其实一样.我们可以使用测试脚本测试下是不是被导出了

+ test.py

```python
import pandas as pd
df = pd.read_parquet('output/users.parquet', engine='pyarrow')
print(df)
```

这个例子是一个最简化的例子,真实场景会有更多的细节,比如数据应该没有id,应该要有创建时间戳等等.

### 监听kafka

这个例子我们模拟监听流,从Kafka读取数据,然后使用`PARQUET`格式写入到文件系统中.

这也是一个常见的需求,比如我们知道ip地址是可以用于判断请求发起对应的城市的.我们要监控比如每2分钟不同地区的请求情况,用于描绘不同地区人的不同使用习惯.这样的话就会用类似的写法.

不同之处是这种信息一般都是要存到一个时间序列数据库中用于监控或者存在hdfs这样的廉价大规模存储中备份的,而这边是写到了文件系统.

> 读写kafka



```shell
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.1 \
--master "local[4]" \
--jars libs/spark-sql-kafka-0-10_2.11-2.4.1.jar \
save_stream.py
```

### 创建事件

这个例子我们模拟流处理事件,从Kafka读取数据,然后判断是否需要发出预警信号,如果要的话就通过redis广播.