package org.luvx;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.luvx.common.value.Const;
import org.luvx.entity.LogEvent;

/**
 * @ClassName: org.luvx.sql
 * @Description:
 * @Author: Ren, Xie
 */
public class LogEventUtils {

    /**
     * 源
     *
     * @param env
     * @return
     */
    public static SingleOutputStreamOperator<LogEvent> source(StreamExecutionEnvironment env) {
        DataStreamSource<String> stream = env.socketTextStream(Const.HOST, Const.PORT, "\n");
        SingleOutputStreamOperator<LogEvent> source = stream.map(
                new MapFunction<String, LogEvent>() {
                    @Override
                    public LogEvent map(String s) throws Exception {
                        String[] tokens = s.toLowerCase().split("\\s+");
                        return LogEvent.of(tokens);
                    }
                }
        );
        return source;
    }
}
