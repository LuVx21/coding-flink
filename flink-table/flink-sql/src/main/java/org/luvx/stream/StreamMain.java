package org.luvx.stream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.luvx.entity.WordWithCount;

import java.util.Arrays;

/**
 * @ClassName: com.xinmei.tdata.dwhtools.task
 * @Description:
 * @Author: Ren, Xie
 */
public class StreamMain {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStream<WordWithCount> stream1 = env.fromCollection(Arrays.asList(
                new WordWithCount("foo", 3),
                new WordWithCount("bar", 4),
                new WordWithCount("flink", 2)));

        DataStream<WordWithCount> stream2 = env.fromCollection(Arrays.asList(
                new WordWithCount("foo1", 3),
                new WordWithCount("bar1", 3),
                new WordWithCount("flink", 1)));

        sql(tEnv, stream1, stream2);
        // table(tEnv, stream1, stream2);

        env.execute();
    }

    /**
     * StreamSql
     *
     * @param tEnv
     * @param stream1
     * @param stream2
     */
    public static void sql(StreamTableEnvironment tEnv, DataStream<WordWithCount> stream1, DataStream<WordWithCount> stream2) {
        Table table1 = tEnv.fromDataStream(stream1, "word, cnt");
        tEnv.registerDataStream("table2", stream2, "word, cnt");

        Table result = tEnv.sqlQuery("select * from " + table1 + " where cnt > 2 union all "
                + "select * from table2 where cnt > 2");

        tEnv.toAppendStream(result, WordWithCount.class).print("sql");
    }

    /**
     * StreamTable
     *
     * @param tEnv
     * @param stream1
     * @param stream2
     */
    public static void table(StreamTableEnvironment tEnv, DataStream<WordWithCount> stream1, DataStream<WordWithCount> stream2) {
        Table table1 = tEnv.fromDataStream(stream1, "word, cnt");
        Table table2 = tEnv.fromDataStream(stream2, "word, cnt");

        Table table = table1.unionAll(table2).select("word, cnt").where("cnt > 2");

        // Table table11 = table1.select("word, cnt").where("cnt > 2");
        // Table table22 = table2.select("word, cnt").where("cnt > 2");
        // table = table11.unionAll(table22);

        tEnv.toAppendStream(table, WordWithCount.class).print("table");
    }
}
