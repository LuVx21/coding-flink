package org.luvx.stream.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.luvx.entity.join.Score;
import org.luvx.entity.join.Student;
import org.luvx.entity.join.StudentScore;
import org.luvx.source.StudentScoreSource;

/**
 * @ClassName: org.luvx.stream.join
 * @Description: 流join, 此外还有table api的join
 * @Author: Ren, Xie
 */
public class JoinMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Student, Score>> source = env.addSource(new StudentScoreSource());

        SingleOutputStreamOperator<Student> student = source.map(new MapFunction<Tuple2<Student, Score>, Student>() {
            @Override
            public Student map(Tuple2<Student, Score> value) throws Exception {
                return value.getField(0);
            }
        });
        SingleOutputStreamOperator<Score> score = source.map(new MapFunction<Tuple2<Student, Score>, Score>() {
            @Override
            public Score map(Tuple2<Student, Score> value) throws Exception {
                return value.getField(1);
            }
        });
        DataStream<StudentScore> ss = join(student, score);
        ss.print("after join");

        env.execute("Join Example");
    }

    /**
     * 合并两个流为元素为新对象的流
     * join 和 coGroup 什么区别?
     *
     * @param student
     * @param score
     */
    private static DataStream<StudentScore> join(SingleOutputStreamOperator<Student> student, SingleOutputStreamOperator<Score> score) {
        long size = 10_000;
        JoinedStreams<Student, Score> jj = student.join(score);
        DataStream<StudentScore> ss = jj.where(new StudentSelector())
                .equalTo(new ScoreSelector())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(size)))
                .apply(
                        new JoinFunction<Student, Score, StudentScore>() {
                            @Override
                            public StudentScore join(Student first, Score second) throws Exception {
                                return StudentScore.of(first.getId(), first.getName(), second.getId(), second.getScore());
                            }
                        }
                );

        return ss;
    }

    private static class StudentSelector implements KeySelector<Student, Integer> {
        @Override
        public Integer getKey(Student value) throws Exception {
            return value.getId();
        }
    }

    private static class ScoreSelector implements KeySelector<Score, Integer> {
        @Override
        public Integer getKey(Score value) throws Exception {
            return value.getSid();
        }
    }
}
