package org.luvx.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.luvx.entity.join.Score;
import org.luvx.entity.join.Student;
import org.luvx.entity.join.StudentScore;
import org.luvx.source.StudentScoreSource;

/**
 * @ClassName: org.luvx.join
 * @Description: ↓
 * 动态表, 静态表
 * 表间join(流join)
 * @Author: Ren, Xie
 */
public class JoinMain {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<Tuple2<Student, Score>> source = env.addSource(new StudentScoreSource());
        SingleOutputStreamOperator<Student> s = source.map(new MapFunction<Tuple2<Student, Score>, Student>() {
            @Override
            public Student map(Tuple2<Student, Score> value) throws Exception {
                return value.getField(0);
            }
        });
        SingleOutputStreamOperator<Score> ss = source.map(new MapFunction<Tuple2<Student, Score>, Score>() {
            @Override
            public Score map(Tuple2<Student, Score> value) throws Exception {
                return value.getField(1);
            }
        });

        /// table
        Table student = tEnv.fromDataStream(s, "id, name");
        Table score = tEnv.fromDataStream(ss, "id, sid, score").as("eid, sid, score");
        Table t = student.join(score, "id = sid")
                .select("id, name, eid, score");

        /// sql
        // tEnv.registerDataStream("student", s, "id, name");
        // tEnv.registerDataStream("score", ss, "id, sid, score");
        // Table t = tEnv.sqlQuery("select a.id, a.name, b.id as eid, b.score from student a, score b where a.id = b.sid");

        tEnv.toAppendStream(t, StudentScore.class).print("result");

        env.execute("table join");
    }
}