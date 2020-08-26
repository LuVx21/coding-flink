package org.luvx.file;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

/**
 * @ClassName: org.luvx.connector.jdbc
 * @Description:
 * @Author: Ren, Xie
 */
public class CsvMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        source(tEnv);
        sink(tEnv);
        // sink1(tEnv);

        tEnv.execute("file");
    }

    private static void source(StreamTableEnvironment tEnv) {
        String path = CsvMain.class.getClassLoader().getResource("data/word.txt").getPath();
        TableSource source = new CsvTableSource(
                path,
                new String[]{"word"},
                new TypeInformation[]{Types.STRING}
        );
        tEnv.registerTableSource("t_source", source);
    }

    /**
     * sink需要实现
     * {@link org.apache.flink.table.sinks.RetractStreamTableSink}
     * 或者
     * {@link org.apache.flink.table.sinks.UpsertStreamTableSink} 表需要有unique key
     *
     * @param tEnv
     */
    private static void sink(StreamTableEnvironment tEnv) {
        RetractStreamTableSink<Row> sink = new CsvRetractStreamTableSink(
                new String[]{"word", "_count"},
                new TypeInformation[]{Types.STRING, Types.LONG}
        );
        tEnv.registerTableSink("t_sink", sink);

        Table t = tEnv.sqlQuery("select word, count(word) as _count from t_source group by word");
        t.insertInto("t_sink");
    }

    /**
     * 不能这样使用
     * 会出现以下异常
     * <pre>
     *     org.apache.flink.table.api.TableException:
     *     AppendStreamTableSink requires that Table has only insert changes.
     * </pre>
     * 因为CsvTableSink实现的是 {@link org.apache.flink.table.sinks.AppendStreamTableSink}接口
     * 仅支持插入更改, 不支持更新或删除更改
     *
     * @param tEnv
     */
    private static void sink1(StreamTableEnvironment tEnv) {
        CsvTableSink sink = new CsvTableSink(
                "./word.csv",
                ",",
                1,
                FileSystem.WriteMode.OVERWRITE);

        sink = (CsvTableSink) sink.configure(
                new String[]{"word", "_count"},
                new TypeInformation[]{Types.STRING, Types.LONG}
        );

        tEnv.registerTableSink("t_sink", sink);
        Table t = tEnv.sqlQuery("select word, count(word) as _count from t_source group by word");
        t.insertInto("t_sink");
    }
}
