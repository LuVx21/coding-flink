package org.luvx.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.luvx.entity.WordWithCount;

/**
 * @ClassName: org.luvx.batch
 * @Description:
 * @Author: Ren, Xie
 */
public class WordCountMain {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.fromElements("foo", "bar", "foo");

        method0(text);
        method1(text);
    }

    private static void method0(DataSet<String> text) throws Exception {
        DataSet<WordWithCount> counts = text.flatMap(
                new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String s, Collector<WordWithCount> collector) throws Exception {
                        String[] tokens = s.toLowerCase().split("\\W+");
                        for (String token : tokens) {
                            if (token.length() > 0) {
                                collector.collect(new WordWithCount(token, 1));
                            }
                        }
                    }
                }).groupBy("word")
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount value1, WordWithCount value2) throws Exception {
                        return new WordWithCount(value1.word, value1.cnt + value2.cnt);
                    }
                });

        counts.print();
    }

    private static void method1(DataSet<String> text) throws Exception {
        DataSet<Tuple2<String, Integer>> counts = text.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] tokens = s.toLowerCase().split("\\W+");
                        for (String token : tokens) {
                            if (token.length() > 0) {
                                collector.collect(new Tuple2<>(token, 1));
                            }
                        }
                    }
                }).groupBy(0)
                .sum(1);

        counts.print();
    }
}
