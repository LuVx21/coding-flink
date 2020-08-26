package org.luvx.common.utils;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * @ClassName: org.luvx.utils
 * @Description:
 * @Author: Ren, Xie
 */
@Slf4j
public class FlinkUtils {
    /**
     * json格式输出(查看事件内容用)
     *
     * @param operator0
     */
    @SuppressWarnings("unchecked")
    public static void print(SingleOutputStreamOperator operator0) {
        SingleOutputStreamOperator<String> operator = operator0.map(
                new MapFunction<Object, String>() {
                    @Override
                    public String map(Object value) throws Exception {
                        return JSON.toJSONString(value);
                    }
                }
        );
        operator.print();
    }
}
