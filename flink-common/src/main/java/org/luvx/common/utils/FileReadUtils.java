package org.luvx.common.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;

/**
 * @ClassName: org.luvx.common.utils
 * @Description:
 * @Author: Ren, Xie
 */
public class FileReadUtils {

    /**
     * resources 目录下开始
     *
     * @param path   路径
     * @param prefix 注释符号, 无则为null
     * @return
     */
    public static String readFile(String path, String prefix) {
        StringBuilder sb = new StringBuilder();
        try (InputStream inputStream = FileReadUtils.class.getClassLoader().getResourceAsStream(path)) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                while (reader.ready()) {
                    String line = null;
                    while ((line = reader.readLine()) != null) {
                        if (prefix != null && line.startsWith(prefix)) {
                            continue;
                        }
                        sb.append(line + System.lineSeparator());
                    }
                }
            }
        } catch (IOException e) {
        } finally {
            return sb.toString();
        }
    }

    public static List<String> readLines(String path, String prefix) {
        /// NIO
        List<String> lines = null;
        try {
            lines = Files.readAllLines(Paths.get(path));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Iterator<String> it = lines.iterator();
        while (it.hasNext()) {
            String line = it.next();
            if (prefix != null && line.startsWith(prefix)) {
                it.remove();
            }
        }
        return lines;
    }
}
