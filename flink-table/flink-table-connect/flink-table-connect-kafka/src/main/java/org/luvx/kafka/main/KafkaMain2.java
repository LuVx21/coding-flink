package org.luvx.kafka.main;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.luvx.common.utils.FileReadUtils;

/**
 * @ClassName: org.luvx.env
 * @Description: kafka -> mysql
 * @Author: Ren, Xie
 */
public class KafkaMain2 {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        String ddl1 = readFile("sql/query1.sql");
        String ddl2 = readFile("sql/query2.sql");
        String dml = readFile("sql/query3.sql");

        tEnv.sqlUpdate(ddl1);
        tEnv.sqlUpdate(ddl2);
        tEnv.sqlUpdate(dml);

        tEnv.execute("SQL Job");
    }

    private static String readFile(String fileName) {
        String sql = FileReadUtils.readFile(fileName, "--");
        sql = sql.trim();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1).trim();
        }
        return sql;
    }
}
