package org.luvx.file.function;

import lombok.Data;

/**
 * @ClassName: org.luvx.entity
 * @Description:
 * @Author: Ren, Xie
 */
@Data
public class ItemViewCount {
    /**
     * 商品 ID
     */
    private long itemId;

    /**
     * 窗口结束时间戳
     */
    private long windowEnd;

    /**
     * 商品点击数
     */
    private long viewCount;

    public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
        ItemViewCount result = new ItemViewCount();
        result.setItemId(itemId);
        result.setWindowEnd(windowEnd);
        result.setViewCount(viewCount);
        return result;
    }
}
