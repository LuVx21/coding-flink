package org.luvx.entity;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * @ClassName: org.luvx.common.entity
 * @Description:
 * @Author: Ren, Xie
 */
@AllArgsConstructor
@NoArgsConstructor
public class WordWithCount {
    public String word;
    public long   cnt;

    @Override
    public String toString() {
        return "WordWithCount:" +
                "word='" + word + '\'' +
                ", cnt=" + cnt;
    }
}