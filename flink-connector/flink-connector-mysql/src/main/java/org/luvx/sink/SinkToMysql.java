package org.luvx.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.luvx.entity.UserBehaviorEvent;
import org.luvx.common.utils.JdbcUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @ClassName: org.luvx.sink
 * @Description:
 * @Author: Ren, Xie
 */
public class SinkToMysql extends RichSinkFunction<UserBehaviorEvent> {
    private Connection        connection;
    private PreparedStatement stmt;

    @Override
    public void invoke(UserBehaviorEvent value, Context context) throws Exception {
        stmt.setLong(1, value.getUserId());
        stmt.setLong(2, value.getItemId());
        stmt.setInt(3, value.getCategoryId());
        stmt.setString(4, value.getBehavior());
        stmt.setLong(5, value.getTs());
        stmt.executeUpdate();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = JdbcUtils.getWConn();
        String sql = "insert into user_behavior1 values (?, ?, ?, ?, ?);";
        stmt = connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (stmt != null) {
            stmt.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
