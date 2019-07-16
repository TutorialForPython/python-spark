# TutorialForSpark

spark是当前数据科学处理大数据的基本框架之一,它是开源的.它为处理单机无法处理规模的数据提供了方便.这些处理可能是简单的统计,可能是复杂的图模型.可能是静态的离线数据,也可能是基于流的实时数据.spark是一个一站式的工具包,它为各种场景提供了支持.

## spark环境部署

spark是运行在jvm上的机器,只要有java8环境就可以运行.在[官网](http://spark.apache.org/)下载好后设置好环境变量`JAVA_HOME`,`SPARK_HOME`,并将`SPARK_HOME`下的`bin`文件夹加入`PATH`就已经可以运行了.

spark支持4种运行模式:

+ 本地单机模式.这种模式使用多进程模拟多机情况,主要用于学习和调试
+ standalone模式.这种模式不依赖任何其他环境,使用spark内置的任务调度器通过网络直接构建分布式集群.
+ 基于mesos的分布式集群模式:使用mesos作为资源管理的基础搭建分布式集群.
+ 基于YARN的分布式集群模式:使用hadoop2的YARN作为资源管理的基础设施,搭建分布式集群.这种方式可以看我之前的博文[基于树莓派的集群实验](http://blog.hszofficial.site/introduce/2016/11/26/%E6%A0%91%E8%8E%93%E6%B4%BE%E9%85%8D%E7%BD%AEspark_on_yarn/).

本攻略介绍spark的相关使用方式.分为

+ spark的开发环境搭建
+ 使用spark做静态数据处理
+ 使用spark做机器学习
+ 使用spark做图算法
+ 使用spark做流式数据处理

spark支持scala,java,python,R4种语言本文将使用其python接口,配合python拥有的大量的数据处理工具来做静态分析.

我们的例子都将在单机模式下运行.一般搭建集群环境是由运维人员完成的.数据科学家并不需要关心.
