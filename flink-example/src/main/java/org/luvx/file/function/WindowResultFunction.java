package org.luvx.file.function;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ClassName: org.luvx.entity
 * @Description:
 * @Author: Ren, Xie
 */
@SuppressWarnings("unchecked")
public class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
    /**
     * 窗口函数
     *
     * @param tuple  窗口的主键，即 itemId
     * @param window 窗口
     * @param input  聚合函数，即 count 值
     * @param out    输出类型是 ItemViewCount
     * @throws Exception
     */
    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out)
            throws Exception {
        Long itemId = ((Tuple1<Long>) tuple).f0;
        Long count = input.iterator().next();
        out.collect(ItemViewCount.of(itemId, window.getEnd(), count));
    }
}