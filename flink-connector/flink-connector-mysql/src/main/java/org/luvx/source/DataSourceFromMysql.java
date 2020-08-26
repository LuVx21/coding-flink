package org.luvx.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.luvx.entity.UserBehaviorEvent;
import org.luvx.common.utils.JdbcUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @ClassName: org.luvx.source
 * @Description:
 * @Author: Ren, Xie
 */
@Slf4j
public class DataSourceFromMysql extends RichSourceFunction<UserBehaviorEvent> {
    private Connection        conn;
    private PreparedStatement stmt;

    @Override
    public void run(SourceContext<UserBehaviorEvent> sourceContext) {
        for (; ; ) {
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    UserBehaviorEvent u = UserBehaviorEvent.builder()
                            .userId(rs.getLong("user_id"))
                            .itemId(rs.getLong("item_id"))
                            .categoryId(rs.getInt("category_id"))
                            .behavior(rs.getString("behavior"))
                            .ts(rs.getLong("timestamp"))
                            .build();
                    sourceContext.collect(u);
                }
            } catch (SQLException e) {
                log.error("sql执行异常");
            }
            try {
                Thread.sleep(10_000);
            } catch (InterruptedException e) {
                log.error("线程中断异常");
            }
        }
    }

    @Override
    public void cancel() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = JdbcUtils.getWConn();
        String sql = "select * from user_behavior order by user_id;";
        stmt = this.conn.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (conn != null) {
            conn.close();
        }
        if (stmt != null) {
            stmt.close();
        }
    }
}
