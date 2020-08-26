package org.luvx.file.function;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;


/**
 * @ClassName: org.luvx.entity
 * @Description:
 * @Author: Ren, Xie
 */
public class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

    private final int topSize;

    /**
     * 用于存储商品与点击数的状态
     * 待收取同一个窗口的数据后，再触发 TopN 计算
     */
    private ListState<ItemViewCount> itemState;

    public TopNHotItems(int topSize) {
        this.topSize = topSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 重载 open 方法，注册状态
        ListStateDescriptor<ItemViewCount> itemViewCountListStateDescriptor =
                new ListStateDescriptor<>(
                        "itemState-state",
                        ItemViewCount.class
                );
        // 用来存储收到的每条 ItemViewCount 状态，保证在发生故障时，状态数据的不丢失和一致性
        itemState = getRuntimeContext().getListState(itemViewCountListStateDescriptor);
    }

    @Override
    public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
        // 每条数据都保存到状态中
        itemState.add(value);
        // 注册 windowEnd + 1 的 EventTime Timer，当触发时，说明收起了属于 windowEnd 窗口的所有数据
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        // 获取收到的所有商品点击量
        List<ItemViewCount> allItems = new ArrayList<>();
        Iterable<ItemViewCount> it = itemState.get();
        for (ItemViewCount a : it) {
            allItems.add(a);
        }

        // 提前清除状态中的数据，释放空间
        itemState.clear();
        // 按照点击量从大到小排序（也就是按照某字段正向排序，然后进行反序）
        allItems.sort(Comparator.comparing(ItemViewCount::getViewCount).reversed());
        // 将排名信息格式化成 String
        StringBuilder result = new StringBuilder();
        result.append("\n================================== TEST =================================\n");
        result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n");
        // 遍历点击事件到结果中
        int realSize = Math.min(allItems.size(), topSize);
        for (int i = 0; i < realSize; i++) {
            ItemViewCount item = allItems.get(i);
            if (item == null) {
                continue;
            }
            result.append("No ").append(i + 1).append(":")
                    .append("    商品ID=").append(item.getItemId())
                    .append("    浏览量=").append(item.getViewCount())
                    .append("\n");
        }
        result.append("================================== END =================================\n\n");
        out.collect(result.toString());
    }
}