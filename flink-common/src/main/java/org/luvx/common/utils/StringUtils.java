package org.luvx.common.utils;

/**
 * @ClassName: org.luvx.common.utils
 * @Description:
 * @Author: Ren, Xie
 */
public class StringUtils {

    public static String merge(String... array) {
        StringBuilder sb = new StringBuilder();
        for (String s : array) {
            sb.append(s);
        }
        return sb.toString();
    }
}
