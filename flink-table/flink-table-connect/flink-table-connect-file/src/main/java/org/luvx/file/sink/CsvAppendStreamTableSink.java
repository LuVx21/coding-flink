package org.luvx.file.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/**
 * @author: Ren, Xie
 * @desc:
 */
public class CsvAppendStreamTableSink implements AppendStreamTableSink<Row> {
    String[]          fieldNames;
    TypeInformation[] fieldTypes;

    public CsvAppendStreamTableSink() {
    }

    public CsvAppendStreamTableSink(String[] fieldNames, TypeInformation[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        consumeDataStream(dataStream);
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
        return dataStream.addSink(
                new SinkFunction<Row>() {
                    @Override
                    public void invoke(Row value, Context context) throws Exception {
                        System.out.println(value);
                    }
                }
        );
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return new CsvAppendStreamTableSink(fieldNames, fieldTypes);
    }
}
