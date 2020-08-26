package org.luvx.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.luvx.common.value.Const;
import org.luvx.entity.WordWithCount;

/**
 * @ClassName: org.luvx.wordcount
 * @Description: nc -lk 9000
 * @Author: Ren, Xie
 */
public class WordCountMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream(Const.HOST, Const.PORT);

        // count0(source);
        count1(source);

        env.execute("Word Count Example");
    }

    private static void count0(DataStreamSource<String> source) {
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = source
                .flatMap(
                        new FlatMapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
                                String[] tokens = s.toLowerCase().split("\\s+");
                                for (String token : tokens) {
                                    if (token.length() > 0) {
                                        out.collect(new Tuple2<>(token, 1));
                                    }
                                }
                            }
                        }
                )
                .keyBy(0)
                .sum(1);
        operator.print("count0");
    }

    private static void count1(DataStreamSource<String> source) {
        SingleOutputStreamOperator<WordWithCount> operator = source.flatMap(
                new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String s, Collector<WordWithCount> out) throws Exception {
                        String[] tokens = s.toLowerCase().split("\\s+");
                        for (String token : tokens) {
                            out.collect(new WordWithCount(token, 1));
                        }
                    }
                }
        )
                .keyBy("word")
                .timeWindow(Time.seconds(5))
                .reduce(
                        new ReduceFunction<WordWithCount>() {
                            @Override
                            public WordWithCount reduce(WordWithCount value1, WordWithCount value2) throws Exception {
                                return new WordWithCount(value1.word, value1.cnt + value2.cnt);
                            }
                        }
                );
        operator.print("count1").setParallelism(1);
    }
}
