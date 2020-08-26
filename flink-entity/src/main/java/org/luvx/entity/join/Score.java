package org.luvx.entity.join;

import lombok.Data;

/**
 * @ClassName: org.luvx.join.entity
 * @Description:
 * @Author: Ren, Xie
 */
@Data
public class Score {
    private Integer id;
    private String  name;
    /**
     * student 的 id
     */
    private Integer sid;
    private Integer score;
    private long    ssTime;
}
