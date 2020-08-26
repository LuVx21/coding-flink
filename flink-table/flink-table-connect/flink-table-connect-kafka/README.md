

连接Kafka有4种方式

1. `connector` 中提供的继承自 `ConnectorDescriptor` 的类 `Kafka` 
2. DDL with 语句
3. 实现了 `StreamTableSource` 接口的 `KafkaTableSource`
4. 类似3, 自定义一个 `KafkaTableSource`
