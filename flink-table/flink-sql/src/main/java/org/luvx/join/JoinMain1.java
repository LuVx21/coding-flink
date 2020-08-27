package org.luvx.join;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.luvx.common.utils.StringUtils;
import org.luvx.entity.join.Score;
import org.luvx.entity.join.Student;

/**
 * @ClassName: org.luvx.join
 * @Description: 双流 流中事件重复时 join 的问题
 * 双流 join 时, 某个流中的某个数据 出现了两次 导致的重复计算
 * 测试用数据, 数字是输入顺序
 * <pre>
 *     student:
 * 1    11,foo
 * 4    11,foo1
 *     score:
 * 2    100,math,11,97
 * 3    101,english,11,98
 * </pre>
 * @Author: Ren, Xie
 */
public class JoinMain1 {

    private static final String host  = "luvx";
    private static final int    port  = 59000;
    private static final int    port1 = 59001;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        SingleOutputStreamOperator<Student> student = env.socketTextStream(host, port, "\n")
                .map(new MapFunction<String, Student>() {
                         @Override
                         public Student map(String s) throws Exception {
                             String[] tokens = s.toLowerCase().split(",");
                             Student ss = new Student();
                             ss.setId(Integer.valueOf(tokens[0]));
                             ss.setName(tokens[1]);
                             return ss;
                         }
                     }
                );

        SingleOutputStreamOperator<Score> score = env.socketTextStream(host, port1, "\n")
                .map(new MapFunction<String, Score>() {
                         @Override
                         public Score map(String s) throws Exception {
                             String[] tokens = s.toLowerCase().split(",");
                             Score ss = new Score();
                             ss.setId(Integer.valueOf(tokens[0]));
                             ss.setName(tokens[1]);
                             ss.setSid(Integer.valueOf(tokens[2]));
                             ss.setScore(Integer.valueOf(tokens[3]));
                             return ss;
                         }
                     }
                );

        /// sql
        tEnv.registerDataStream("student", student, "id, name, sTime, proctime.proctime");
        tEnv.registerDataStream("score", score, "id, name, sid, score, ssTime");

        // Table t = tEnv.sqlQuery("select sum(t2.score) from student t1 inner join score t2 on t1.id = t2.sid");

        String sql = StringUtils.merge(
                " select",
                "     sum(t2.score) as cnt",
                " from",
                " (",
                "     select *",
                "     from",
                "         (",
                "             select",
                "                 *, row_number() over (partition by id order by proctime desc) as row_num",
                "             from student",
                "         )",
                "     where row_num = 1",
                " ) as t1 inner join score t2 on t1.id = t2.sid"
        );

        Table t = tEnv.sqlQuery(sql);

        tEnv.toRetractStream(t, Row.class).print("result");

        env.execute("table join");
    }
}