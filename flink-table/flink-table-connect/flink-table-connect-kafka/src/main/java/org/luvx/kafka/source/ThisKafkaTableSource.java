package org.luvx.kafka.source;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.luvx.common.utils.KafkaConfigUtils;
import org.luvx.kafka.Info;

/**
 * @package: org.luvx.kafka.source
 * @author: Ren, Xie
 * @desc: 自定义数据源
 * @see org.apache.flink.streaming.connectors.kafka.KafkaTableSource
 * @see org.apache.flink.streaming.connectors.kafka.KafkaTableSourceBase
 * <p>
 * TODO
 * 多列的怎么处理, 如多列的csv格式, json格式
 */
public class ThisKafkaTableSource implements StreamTableSource<Row> {
    @Override
    public DataType getProducedDataType() {
        return DataTypes.ROW();
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment env) {
        return env.addSource(
                new FlinkKafkaConsumer<>(
                        Info.topic,
                        new JsonRowDeserializationSchema.Builder(
                                new RowTypeInfo(
                                        TypeConversions.fromDataTypeToLegacyInfo(Info.types),
                                        Info.fields
                                )
                        ).build(),
                        KafkaConfigUtils.getConsumerProp()
                )
        );
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder()
                .fields(Info.fields, Info.types)
                .build();
    }
}
