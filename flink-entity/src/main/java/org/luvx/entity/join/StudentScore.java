package org.luvx.entity.join;

import lombok.Data;

/**
 * @ClassName: org.luvx.stream.join
 * @Description: student 和 score join 后
 * @Author: Ren, Xie
 */
@Data
public class StudentScore {
    private Integer id;
    private String  name;
    private Integer eid;
    private Integer score;

    public static StudentScore of(Integer id, String name, Integer eid, Integer score) {
        StudentScore ss = new StudentScore();
        ss.setId(id);
        ss.setName(name);
        ss.setEid(eid);
        ss.setScore(score);
        return ss;
    }
}
