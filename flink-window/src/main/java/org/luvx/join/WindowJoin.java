package org.luvx.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.luvx.join.WindowJoinSampleData.GradeSource;
import org.luvx.join.WindowJoinSampleData.SalarySource;

/**
 * @ClassName: org.luvx.join
 * @Description:
 * @Author: Ren, Xie
 */
public class WindowJoin {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final long windowSize = params.getLong("windowSize", 2000);
        final long rate = params.getLong("rate", 3L);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<Tuple2<String, Integer>> grades = GradeSource.getSource(env, rate);
        DataStream<Tuple2<String, Integer>> salaries = SalarySource.getSource(env, rate);
        DataStream<Tuple3<String, Integer, Integer>> target = join(grades, salaries, windowSize);

        target.print("window join").setParallelism(1);

        env.execute("Windowed Join Example");
    }

    public static DataStream<Tuple3<String, Integer, Integer>> join(
            DataStream<Tuple2<String, Integer>> grades,
            DataStream<Tuple2<String, Integer>> salaries,
            long windowSize) {
        return grades.join(salaries)
                .where(new NameKeySelector())
                .equalTo(new NameKeySelector())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> join(
                            Tuple2<String, Integer> first,
                            Tuple2<String, Integer> second) {
                        return new Tuple3<>(first.f0, first.f1, second.f1);
                    }
                });
    }

    private static class NameKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
        @Override
        public String getKey(Tuple2<String, Integer> value) {
            return value.f0;
        }
    }
}
