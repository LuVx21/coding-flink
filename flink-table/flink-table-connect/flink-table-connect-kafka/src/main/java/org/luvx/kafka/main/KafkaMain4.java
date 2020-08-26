package org.luvx.kafka.main;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.luvx.kafka.source.ThisKafkaTableSource;

/**
 * @package: org.luvx.kafka
 * @author: Ren, Xie
 * @desc: 自定义 tableSource tableSink
 */
public class KafkaMain4 {
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
        tEnv.registerTableSource("t_source", new ThisKafkaTableSource());
        Table t1 = tEnv.sqlQuery("select * from t_source");
        tEnv.toAppendStream(t1, Row.class).print("source");
    }

    private static void sink(StreamTableEnvironment tEnv) {
    }
}
