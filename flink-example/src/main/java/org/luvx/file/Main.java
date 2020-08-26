package org.luvx.file;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;
import org.luvx.entity.UserBehaviorEvent;

import java.net.URL;

/**
 * @ClassName: org.luvx
 * @Description:
 * @Author: Ren, Xie
 */
public class Main {
    private static final String LOCAL_LOCATION = "data/UserBehaviorTest.txt";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        URL url = Main.class.getClassLoader().getResource(LOCAL_LOCATION);
        String filePath = url.getPath();

        // compute1(env, filePath);
        compute2(env, filePath);

        env.execute("compute Hot Items");
    }

    private static void compute1(StreamExecutionEnvironment env, String filePath) {
        DataStreamSource<String> streamSource = env.readTextFile(filePath);
        streamSource.setParallelism(1);

        SingleOutputStreamOperator<UserBehaviorEvent> operator = streamSource.map(
                new MapFunction<String, UserBehaviorEvent>() {
                    @Override
                    public UserBehaviorEvent map(String s) throws Exception {
                        String[] tokens = s.split("\\W+");
                        return UserBehaviorEvent.of(tokens);
                    }
                }
        );
        operator.print("111");
    }

    private static void compute2(StreamExecutionEnvironment env, String filePath) {
        TextInputFormat inputFormat = new TextInputFormat(new Path(filePath));
        DataStreamSource<String> streamSource = env.readFile(
                inputFormat, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY,
                100L, TypeExtractor.getInputFormatTypes(inputFormat));
        streamSource.setParallelism(1);

        SingleOutputStreamOperator<UserBehaviorEvent> operator = streamSource.flatMap(
                new FlatMapFunction<String, UserBehaviorEvent>() {
                    @Override
                    public void flatMap(String s, Collector<UserBehaviorEvent> collector) throws Exception {
                        String[] tokens = s.split("\\W+");
                        if (tokens.length > 1) {
                            collector.collect(UserBehaviorEvent.of(tokens));
                        }
                    }
                }
        );
        operator.print("222");
    }
}
