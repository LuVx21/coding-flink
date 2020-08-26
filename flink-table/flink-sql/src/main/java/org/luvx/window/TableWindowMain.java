package org.luvx.window;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.luvx.LogEventUtils;
import org.luvx.common.utils.StringUtils;
import org.luvx.entity.LogEvent;

import java.sql.Timestamp;

/**
 * @ClassName: org.luvx.window
 * @Description: 数据↓
 * <pre>
 *     83.149.9.216 - - 17/05/2015:10:05:01 +0000 GET /favicon.ico
 * </pre>
 * @Author: Ren, Xie
 */
public class TableWindowMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<LogEvent> source = LogEventUtils.source(env);

        /// source.print("事件");

        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // SingleOutputStreamOperator<LogEvent> operator = source.assignTimestampsAndWatermarks(
        //         new AscendingTimestampExtractor<LogEvent>() {
        //             @Override
        //             public long extractAscendingTimestamp(LogEvent element) {
        //                 return element.getEventTime();
        //             }
        //         }
        // );

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        sql(tEnv, source);

        env.execute("table window");
    }

    private static void sql(StreamTableEnvironment tEnv, SingleOutputStreamOperator<LogEvent> source) {
        tEnv.registerDataStream("log", source, "ip, eventTime, timeZone, type, url, proctime.proctime");
        sql0(tEnv);
    }

    private static void sql0(StreamTableEnvironment tEnv) {
        String sql = StringUtils.merge(
                "select ip, count(1) ",
                " from log ",
                " group by tumble(proctime, interval '30' second), ip"
        );

        Table result = tEnv.sqlQuery(sql);
        tEnv.toAppendStream(result, Row.class).print("result");
    }

    /**
     * <pre>
     * select
     *   tumble_start(eventTime, interval '10' second) as window_start,
     *   tumble_end(eventTime, interval '10' second) as window_end,
     *   count(url)
     * from log
     * group by tumble(eventTime, interval '10' second)
     * </pre>
     *
     * @param tEnv
     */
    private static void sql1(StreamTableEnvironment tEnv) {
        String sql = StringUtils.merge(
                "select",
                "   tumble_start(eventTime, interval '10' second) as window_start,",
                "   tumble_end(eventTime, interval '10' second) as window_end,",
                "   count(url)",
                " from log",
                " group by tumble(eventTime, interval '10' second)"
        );

        Table result = tEnv.sqlQuery(sql);

        TypeInformation<Tuple3<Timestamp, Timestamp, Long>> type =
                new TypeHint<Tuple3<Timestamp, Timestamp, Long>>() {
                }.getTypeInfo();

        tEnv.toAppendStream(result, type).print("result");
    }
}
