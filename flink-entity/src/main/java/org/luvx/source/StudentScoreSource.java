package org.luvx.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.luvx.entity.join.Score;
import org.luvx.entity.join.Student;

import java.util.Random;

/**
 * @ClassName: org.luvx.join
 * @Description: student score 造数据
 * @Author: Ren, Xie
 */
public class StudentScoreSource extends RichSourceFunction<Tuple2<Student, Score>> {
    @Override
    public void run(SourceContext<Tuple2<Student, Score>> ctx) throws Exception {
        for (; ; ) {
            Random r = new Random();
            int i = r.nextInt(Integer.MAX_VALUE - 1);

            Student s = new Student();
            s.setId(i);
            s.setName("name:" + i);

            Score ss = new Score();
            ss.setId(i - 1);
            ss.setSid(i);
            ss.setScore(i + 1);

            Tuple2<Student, Score> pair = new Tuple2<>(s, ss);
            ctx.collect(pair);
            Thread.sleep(10_000);
        }
    }

    @Override
    public void cancel() {
    }
}
