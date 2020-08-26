package org.luvx.common.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * @ClassName: org.luvx.utils
 * @Description:
 * @Author: Ren, Xie
 */
@Slf4j
public class PropertiesUtils {
    /**
     * 文件中获取配置
     *
     * @param path 配置文件路径(resources目录起始)
     * @return 配置
     */
    public static Properties getProperties(String path) {
        InputStream is = PropertiesUtils.class.getClassLoader().getResourceAsStream(path);
        return getProperties(is);
    }

    /**
     * 流中获取配置
     *
     * @param is
     * @return
     */
    private static Properties getProperties(InputStream is) {
        try {
            Properties props = new Properties();
            props.load(is);
            return props;
        } catch (IOException e) {
            log.error("加载配置文件异常", e);
        }
        return null;
    }

    /**
     * 获取指定前缀的配置
     *
     * @param pro
     * @param prefix
     * @return
     */
    public static Properties getPropertiesByPrefix(Properties pro, String prefix) {
        Properties temp = new Properties();
        Set<Map.Entry<Object, Object>> set = pro.entrySet();
        for (Map.Entry<Object, Object> entry : set) {
            String key = (String) entry.getKey();
            if (key.startsWith(prefix)) {
                temp.put(key.substring(prefix.length()), entry.getValue());
            }
        }
        return temp;
    }
}
