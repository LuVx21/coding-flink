package org.luvx.window;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.luvx.entity.LogEvent;

import javax.annotation.Nullable;

/**
 * @ClassName: org.luvx.window
 * @Description:
 * @Author: Ren, Xie
 */
public class Watermarks1 implements AssignerWithPeriodicWatermarks<LogEvent> {
    private       Long currentMaxTimestamp = 0L;
    /**
     * 最大允许乱序时间
     * 窗口结束时间后再加上这个时间开始关闭窗口进行计算
     */
    private final Long maxOutOfOrderness   = 10_000L;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(LogEvent element, long previousElementTimestamp) {
        long time = element.getEventTime();
        currentMaxTimestamp = Math.max(time, previousElementTimestamp);
        /// getCurrentWatermark().getTimestamp();
        return time;
    }
}
