package org.luvx.side;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.luvx.common.value.Const;
import org.luvx.entity.LogEvent;

/**
 * @ClassName: org.luvx.side
 * @Description:
 * @Author: Ren, Xie
 */
public class SideOutMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream(Const.HOST, Const.PORT, "\n");
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<LogEvent> operator = stream.map(
                new MapFunction<String, LogEvent>() {
                    @Override
                    public LogEvent map(String s) throws Exception {
                        String[] tokens = s.toLowerCase().split("\\s+");
                        return LogEvent.of(tokens);
                    }
                }
        );
        operator.print("src");

        sideOut(operator);

        env.execute("test example");
    }

    /**
     * 侧输出
     *
     * @param operator
     */
    private static void sideOut(SingleOutputStreamOperator<LogEvent> operator) {
        /// 必须使用匿名内部类的形式
        OutputTag<LogEvent> outputTag = new OutputTag<LogEvent>("side-output") {
        };

        SingleOutputStreamOperator<LogEvent> operator1 = operator.process(new ProcessFunction<LogEvent, LogEvent>() {
            @Override
            public void processElement(LogEvent value, Context ctx, Collector<LogEvent> out) throws Exception {
                /// 输出到常规流
                if ("POST".equalsIgnoreCase(value.getType())) {
                    out.collect(value);
                }
                // GET请求输出到侧输出流
                if ("GET".equalsIgnoreCase(value.getType())) {
                    ctx.output(outputTag, value);
                }
            }
        });
        operator1.print("post");
        DataStream<LogEvent> source = operator1.getSideOutput(outputTag);
        source.print("side-out");
    }
}
