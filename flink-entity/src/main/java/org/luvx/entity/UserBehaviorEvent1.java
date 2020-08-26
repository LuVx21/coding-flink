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
public class UserBehaviorEvent1 {
    /**
     * 用户id
     */
    private Long    user_id;
    /**
     * 商品id
     */
    private Long    item_id;
    /**
     * 商品种类id
     */
    private Integer category_id;
    /**
     * 用户行为
     * pv, buy, cart, fav
     */
    private String  behavior;
    /**
     * 时间戳
     */
    private Long    ts;

    public static UserBehaviorEvent1 of(UserBehaviorEvent u) {
        return UserBehaviorEvent1.builder()
                .user_id(u.getUserId())
                .item_id(u.getItemId())
                .category_id(u.getCategoryId())
                .behavior(u.getBehavior())
                .ts(u.getTs())
                .build();
    }

}
