package org.luvx.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName: org.luvx.entity
 * @Description:
 * @Author: Ren, Xie
 */
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Data
public class UserBehaviorEvent {
    /**
     * 用户id
     */
    private Long    userId;
    /**
     * 商品id
     */
    private Long    itemId;
    /**
     * 商品种类id
     */
    private Integer categoryId;
    /**
     * 用户行为
     * pv, buy, cart, fav
     */
    private String  behavior;
    /**
     * 时间戳
     */
    private Long    ts;

    public static UserBehaviorEvent of(String[] tokens) {
        return UserBehaviorEvent.builder()
                .userId(Long.valueOf(tokens[0]))
                .itemId(Long.valueOf(tokens[1]))
                .categoryId(Integer.valueOf(tokens[2]))
                .behavior(tokens[3])
                .ts(Long.valueOf(tokens[4]))
                .build();
    }

    public static UserBehaviorEvent of(UserBehaviorEvent s) {
        return UserBehaviorEvent.builder()
                .userId(s.getUserId())
                .itemId(s.getItemId())
                .categoryId(s.getCategoryId())
                .behavior(s.getBehavior())
                .ts(s.getTs())
                .build();
    }
}
