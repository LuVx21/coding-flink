package org.luvx.checkpoint;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.luvx.common.utils.CheckPointUtil;
import org.luvx.common.value.Const;
import org.luvx.entity.LogEvent;

/**
 * @ClassName: org.luvx.checkpoint
 * @Description: nc -lk 9000
 * 212.242.185.85 - - 17/05/2015:12:05:01 +0000 GET /favicon.ico1
 * 212.242.185.85 - - 17/05/2015:12:05:02 +0000 GET /favicon.ico2
 * @Author: Ren, Xie
 */
@Slf4j
public class Main {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream(Const.HOST, Const.PORT, "\n");
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

        operator.print("日志");
        /// operator.writeAsText("./log.log", FileSystem.WriteMode.NO_OVERWRITE);

        CheckPointUtil.setCheckpointConfig(env).execute("window test example");
        // env.execute("window test example");
    }
}
