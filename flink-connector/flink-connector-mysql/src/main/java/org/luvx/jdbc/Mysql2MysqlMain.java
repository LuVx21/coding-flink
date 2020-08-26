package org.luvx.jdbc;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName: org.luvx.jdbc
 * @Description:
 * @Author: Ren, Xie
 */
public class Mysql2MysqlMain {
    private static String driverClass = "com.mysql.jdbc.Driver";
    private static String dbUrl       = "jdbc:mysql://127.0.0.1:3306/boot";
    private static String userName    = "root";
    private static String passWord    = "1121";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[]{
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO
        };

        DataStreamSource<Row> source = read(env, fieldTypes);
        write(fieldTypes, source);

        env.execute();
    }

    /**
     * 读
     *
     * @param env
     * @param fieldTypes
     */
    private static DataStreamSource<Row> read(StreamExecutionEnvironment env, TypeInformation<?>[] fieldTypes) {
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes);
        JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(driverClass)
                .setDBUrl(dbUrl)
                .setUsername(userName)
                .setPassword(passWord)
                .setQuery("select * from user_behavior")
                .setRowTypeInfo(rowTypeInfo)
                .finish();
        DataStreamSource<Row> input1 = env.createInput(jdbcInputFormat);
        input1.print("jdbc read");

        return input1;
    }

    /**
     * 写
     *
     * @param fieldTypes
     * @param source
     */
    private static void write(TypeInformation<?>[] fieldTypes, DataStreamSource<Row> source) {
        JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
                .setDrivername(driverClass)
                .setDBUrl(dbUrl)
                .setUsername(userName)
                .setPassword(passWord)
                // Unsupported type: BigInteger
                .setParameterTypes(fieldTypes)
                .setQuery("insert into user_behavior1 values(?,?,?,?,?)")
                // .setQuery("update student set age = ? where name = ?")
                .build();

        sink.emitDataStream(source);
    }
}
