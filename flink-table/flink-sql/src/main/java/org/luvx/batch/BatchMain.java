package org.luvx.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.luvx.entity.WordWithCount;

/**
 * @ClassName: org.luvx.wordcount
 * @Description:
 * @Author: Ren, Xie
 */
public class BatchMain {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSet<WordWithCount> input = env.fromElements(
                new WordWithCount("foo", 1),
                new WordWithCount("bar", 1),
                new WordWithCount("foo", 1));

        sql(tEnv, input);
        // table(tEnv, input);
    }

    public static void sql(BatchTableEnvironment tEnv, DataSet<WordWithCount> input) throws Exception {
        tEnv.registerDataSet("WordCount", input, "word, cnt");

        Table table = tEnv.sqlQuery(
                "select word, sum(cnt) as cnt from WordCount group by word having sum(cnt) = 2");

        DataSet<WordWithCount> result = tEnv.toDataSet(table, WordWithCount.class);

        result.print();
    }

    public static void table(BatchTableEnvironment tEnv, DataSet<WordWithCount> input) throws Exception {
        Table table = tEnv.fromDataSet(input);

        Table filtered = table
                .groupBy("word")
                .select("word, cnt.sum as cnt")
                .filter("cnt = 2");

        DataSet<WordWithCount> result = tEnv.toDataSet(filtered, WordWithCount.class);
        result.print();
    }
}

