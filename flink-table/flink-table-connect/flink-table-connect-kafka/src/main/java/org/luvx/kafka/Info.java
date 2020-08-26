package org.luvx.kafka;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

/**
 * @package: org.luvx.kafka
 * @author: Ren, Xie
 * @desc: UserBehaviorEvent 表 信息
 */
public class Info {
    public static final String     topic  = "flink_table2";
    public static final String[]   fields = {
            "user_id",
            "item_id",
            "category_id",
            "behavior",
            "ts"
    };
    public static final DataType[] types  = {
            DataTypes.BIGINT(),
            DataTypes.BIGINT(),
            DataTypes.INT(),
            DataTypes.STRING(),
            DataTypes.BIGINT()
    };
}
