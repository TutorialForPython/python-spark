# Spark的开发环境搭建

spark有两种开发模式:

+ repl模式,通过交互式界面建立spark的运行上下文,执行操作.这种常用于一次性的离线分析.scala可以使用`$SPARK_HOME/bin/spark-shell`进入,python可以使用`$SPARK_HOME/bin/pyspark`进入.这个上下文中全局变量`sc`就是一个`SparkContext`执行上下文对象,如果你有一个spark集群则可以使用[sparkmagic](https://github.com/jupyter-incubator/sparkmagic)借助[Livy](https://livy.incubator.apache.org/)使用jupyter notebook与spark交互.有的公司运维给力可能还会安装[hue](https://github.com/cloudera/hue)来统一管理存储和运算资源

+ app模式,通过使用接口`$SPARK_HOME/bin/spark-submit`来提交编译打包好的python项目.这种方式适合流处理场景,定时任务,需要重复执行的任务等.这种模式下python任务就没有那么麻烦,直接上传脚本就行.
