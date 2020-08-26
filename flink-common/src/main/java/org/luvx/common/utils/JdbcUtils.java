package org.luvx.common.utils;

import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.Properties;

/**
 * @ClassName: org.luvx.utils
 * @Description:
 * @Author: Ren, Xie
 */
@Slf4j
public class JdbcUtils {
    /*
        static {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
            } catch (ClassNotFoundException e) {
                throw new ExceptionInInitializerError("加载驱动失败");
            }
        }
    */

    private static final String args = "?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC&useSSL=true&allowMultiQueries=true&autoReconnect=true";

    public static Connection getRConn() {
        return connection("r.");
    }

    public static Connection getWConn() {
        return connection("w.");
    }

    /**
     * 根据前缀获取数据库连接
     * 读库/写库
     *
     * @param prefix
     * @return
     */
    private static Connection connection(String prefix) {
        Properties pro = PropertiesUtils.getProperties("mysql.properties");
        pro = PropertiesUtils.getPropertiesByPrefix(pro, prefix);
        String url = (String) pro.get("datasource.url");
        String username = (String) pro.get("datasource.username");
        String pwd = (String) pro.get("datasource.password");
        return getConnection(url, username, pwd);
    }

    /**
     * 获取数据库连接
     *
     * @param url
     * @param username
     * @param password
     * @return
     */
    public static Connection getConnection(String url, String username, String password) {
        log.info("获取数据库连接:{} 用户:{}", url, username);
        try {
            return DriverManager.getConnection(url + args, username, password);
        } catch (SQLException e) {
            log.error("获取数据库连接:{} 用户:{}", url, username);
            throw new RuntimeException("获取数据库连接失败, 请检查配置");
        }
    }

    /**
     * 释放资源
     *
     * @param autoCloseable
     */
    public static void close(AutoCloseable autoCloseable) {
        if (autoCloseable != null) {
            try {
                autoCloseable.close();
            } catch (Exception e) {
                log.error("释放资源失败", e);
            }
            autoCloseable = null;
        }
    }

    /**
     * 释放所有资源
     *
     * @param rs
     * @param stmt
     * @param conn
     */
    public static void release(ResultSet rs, Statement stmt, Connection conn) {
        close(rs);
        close(stmt);
        close(conn);
    }
}
