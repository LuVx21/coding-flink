package org.luvx;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.luvx.entity.UserBehaviorEvent;
import org.luvx.sink.SinkToMysql;
import org.luvx.source.DataSourceFromMysql;

/**
 * @ClassName: org.luvx
 * @Description:
 * @Author: Ren, Xie
 */
public class Mysql2MysqlMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<UserBehaviorEvent> stream = env.addSource(new DataSourceFromMysql());

        stream.addSink(new SinkToMysql());
        env.execute("mysql -> mysql");
    }
}
