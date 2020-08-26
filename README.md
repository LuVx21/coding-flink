mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-quickstart-java \
    -DarchetypeVersion=1.8.2 \
    -DgroupId=org.luvx \
    -DartifactId=flink-quickstart-java \
    -Dversion=1.0.1-SNAPSHOT \
    -Dpackage=org.luvx \
    -DinteractiveMode=false



nc -l 9000
./bin/flink run examples/streaming/SocketWindowWordCount.jar --port 9000

./bin/flink run -c cn.sevenyuan.wordcount.SocketTextStreamWordCount target/flink-quick-start-1.0-SNAPSHOT.jar

```sql
drop table if exists `user_behavior`;
create table `user_behavior` (
`user_id` int(20) unsigned not null,
`item_id` int(20) unsigned not null,
`category_id` int(20) unsigned not null,
`behavior`varchar(32) not null default '',
`timestamp` int(20) unsigned not null
) engine=innodb default charset=utf8;
```