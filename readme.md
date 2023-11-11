该项目通过配置数据库连接可以直接将binlog发送到mq
或者自定义的任何地方去

参考MqSink即可

直接打包，然后通过flink 控制台就能直接运行


直接复制源码和pom.xml，不要改动任何东西，不然冲突需要自己解决


https://zhuanlan.zhihu.com/p/426489574
https://ververica.github.io/flink-cdc-connectors/release-2.0/content/connectors/mysql-cdc.html#connector-options
https://repository.apache.org/content/repositories/releases/archetype-catalog.xml
https://javers.org/documentation/diff-examples/
https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/kafka/
