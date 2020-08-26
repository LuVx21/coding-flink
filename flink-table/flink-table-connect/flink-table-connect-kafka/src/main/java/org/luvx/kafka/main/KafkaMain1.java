package org.luvx.kafka.main;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.luvx.entity.UserBehaviorEvent1;

/**
 * @ClassName: org.luvx.env
 * @Description: kafka -> mysql
 * @Author: Ren, Xie
 */
public class KafkaMain1 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        source(tEnv);
        sink(tEnv);

        tEnv.execute("foo");
    }

    private static void source(StreamTableEnvironment tEnv) {
        tEnv.connect(
                new Kafka()
                        .version("universal")
                        .topic("flink_table2")
                        .startFromEarliest()
                        // .property("zookeeper.connect", "192.168.224.129:2181")
                        .property("bootstrap.servers", "192.168.224.129:9092")
        ).withFormat(
                new Json()
                        .deriveSchema()
        ).withSchema(
                new Schema()
                        .field("user_id", Types.LONG)
                        .field("item_id", Types.LONG)
                        .field("category_id", Types.INT)
                        .field("behavior", Types.STRING)
                        .field("ts", Types.LONG)
        )
                .inAppendMode()
                .registerTableSource("user_log");
    }

    private static void sink(StreamTableEnvironment tEnv) {
        JDBCAppendTableSink tableSink = JDBCAppendTableSink.builder()
                .setDrivername("com.mysql.cj.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306/boot?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai")
                .setUsername("root").setPassword("1121")
                .setQuery("INSERT INTO user_behavior(user_id, item_id, category_id, behavior, ts) VALUES (?, ?, ?, ?, ?)")
                .setParameterTypes(Types.LONG, Types.LONG, Types.INT, Types.STRING, Types.LONG)
                .build();
        tableSink = (JDBCAppendTableSink) tableSink.configure(
                new String[]{"user_id", "item_id", "category_id", "behavior", "ts"},
                new TypeInformation[]{Types.LONG, Types.LONG, Types.INT, Types.STRING, Types.LONG}
        );

        tEnv.registerTableSink("user_behavior", tableSink);

        Table t1 = tEnv.sqlQuery("select * from user_log");
        /// TODO 为什么insert不进去
        // t1.insertInto("user_behavior");
        // tEnv.insertInto(t1, "user_behavior");
        // tEnv.sqlUpdate("insert into user_behavior select * from user_log");
        // tEnv.sqlQuery("select * from user_log").insertInto("user_behavior");

        tEnv.toAppendStream(t1, UserBehaviorEvent1.class).print("foo");
    }

    private static void sink1(StreamTableEnvironment tEnv) {
        JDBCAppendTableSink tableSink = JDBCAppendTableSink.builder()
                .setDrivername("com.mysql.cj.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306/boot")
                .setUsername("root").setPassword("1121")
                .setQuery("INSERT INTO pvuv_sink(dt, pv, uv) VALUES (?, ?, ?)")
                .setParameterTypes(Types.STRING, Types.LONG, Types.LONG)
                .build();

        tEnv.registerTableSink("pvuv_sink",
                new String[]{"dt", "pv", "uv"},
                new TypeInformation[]{Types.STRING, Types.LONG, Types.LONG},
                tableSink);

        Table t1 = tEnv.sqlQuery("select " +
                "date_format(ts, 'yyyy-mm-dd hh:00') as dt, count(*) as pv, count(distinct user_id) as uv " +
                "from user_log " +
                "group by date_format(ts, 'yyyy-mm-dd hh:00')");
        t1.insertInto("pvuv_sink");
    }
}
