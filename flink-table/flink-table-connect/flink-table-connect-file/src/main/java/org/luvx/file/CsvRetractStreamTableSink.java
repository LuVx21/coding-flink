package org.luvx.file;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import static org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * @ClassName: org.luvx.connector.jdbc
 * @Description:
 * @Author: Ren, Xie
 */
public class CsvRetractStreamTableSink implements RetractStreamTableSink<Row> {
    private TableSchema tableSchema;

    public CsvRetractStreamTableSink(String[] fieldNames, TypeInformation[] fieldTypes) {
        this(fieldNames, fromLegacyInfoToDataType(fieldTypes));
    }

    public CsvRetractStreamTableSink(String[] fieldNames, DataType[] dataTypes) {
        this.tableSchema = TableSchema.builder().fields(fieldNames, dataTypes).build();
    }

    @Override
    public TableSchema getTableSchema() {
        return tableSchema;
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        return dataStream.addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
            @Override
            public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
                if (value.f0) {
                    // 怎样sink:打印或者写入某处
                    System.out.println(value.f1);
                }
            }
        });
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        DataType[] dataTypes = tableSchema.getFieldDataTypes();
        return new RowTypeInfo(fromDataTypeToLegacyInfo(dataTypes), tableSchema.getFieldNames());
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return new CsvRetractStreamTableSink(fieldNames, fieldTypes);
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        consumeDataStream(dataStream);
    }
}
