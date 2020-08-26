package org.luvx.common.utils;

import lombok.Data;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @ClassName: org.luvx.utils
 * @Description:
 * @Author: Ren, Xie
 */
@Data
public class DateTimeUtils {
    public static final DateTimeFormatter dateTimeFormatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS");
    public static final DateTimeFormatter dateTimeFormatter2 = DateTimeFormatter.ofPattern("dd/MM/yyyy:HH:mm:ss");

    /**
     * long -> str
     *
     * @param time
     * @return
     */
    public static String epochMilli2Time(long time) {
        return dateTimeFormatter1.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneId.systemDefault()));
    }

    /**
     * str -> long
     *
     * @param str
     * @return
     */
    public static long str2EpochMilli(String str) {
        LocalDateTime temp = LocalDateTime.parse(str, dateTimeFormatter2);
        long time = LocalDateTime.from(temp).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        return time;
    }
}
