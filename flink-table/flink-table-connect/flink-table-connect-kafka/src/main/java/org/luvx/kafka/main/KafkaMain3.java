package org.luvx.kafka.main;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSink;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.luvx.common.utils.KafkaConfigUtils;
import org.luvx.kafka.Info;

import java.util.Optional;

/**
 * @package: org.luvx.kafka
 * @author: Ren, Xie
 * @desc:
 */
public class KafkaMain3 {
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
        KafkaTableSource tableSource = new KafkaTableSource(
                TableSchema.builder()
                        .fields(Info.fields, Info.types)
                        .build(),
                Info.topic,
                KafkaConfigUtils.getConsumerProp(),
                new JsonRowDeserializationSchema.Builder(
                        new RowTypeInfo(
                                TypeConversions.fromDataTypeToLegacyInfo(Info.types),
                                Info.fields
                        )
                ).build()
        );

        tEnv.registerTableSource("t_source", tableSource);
        // Table t1 = tEnv.sqlQuery("select * from t_source");
        // tEnv.toAppendStream(t1, Row.class).print("source");
    }

    private static void sink(StreamTableEnvironment tEnv) {
        KafkaTableSink tableSink = new KafkaTableSink(
                TableSchema.builder()
                        .fields(Info.fields, Info.types)
                        .build(),
                Info.topic + "_sink",
                KafkaConfigUtils.getProducerPropNoSerializer(),
                Optional.of(
                        new FlinkKafkaPartitioner<Row>() {
                            @Override
                            public int partition(Row record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
                                return 0;
                            }
                        }
                ),
                new JsonRowSerializationSchema.Builder(
                        new RowTypeInfo(
                                TypeConversions.fromDataTypeToLegacyInfo(Info.types),
                                Info.fields
                        )
                ).build()
        );
        tEnv.registerTableSink("t_sink", tableSink);
        Table t1 = tEnv.sqlQuery("select * from t_source");
        t1.insertInto("t_sink");
    }
}
