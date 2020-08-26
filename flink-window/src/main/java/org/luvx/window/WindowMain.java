package org.luvx.window;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.luvx.common.utils.DateTimeUtils;
import org.luvx.common.value.Const;
import org.luvx.entity.LogEvent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @ClassName: org.luvx
 * @Description: nc -lk 9000  使用 request_log.txt 中数据
 * 窗口等待3秒, 窗口大小为10s, 每5s计算一次
 * 水位线 会随着 事件时间的增长而增长
 * <pre>
 *     测试数据事件以每秒形式进入, 起点为2015-05-17 10:05:01:000
 *
 * 结果:
 *    | 触发窗口时间 |    窗口范围 start ~ end     |       事件范围      | 个数 |
 *    | 10:05:08:000 | 10:04:55:000 ~ 10:05:05:000 | 10:05:01 ~ 10:05:04 |  4   |
 *    | 10:05:13:000 | 10:05:00:000 ~ 10:05:10:000 | 10:05:01 ~ 10:05:09 |  9   |
 *    | 10:05:18:000 | 10:05:05:000 ~ 10:05:15:000 | 10:05:05 ~ 10:05:14 | 10   |
 *
 * 时间恰好在window end上的事件不纳入窗口的计算
 * </pre>
 * @Author: Ren, Xie
 */
@Slf4j
public class WindowMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream(Const.HOST, Const.PORT, "\n");
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        SingleOutputStreamOperator<LogEvent> operator = stream.map(
                new MapFunction<String, LogEvent>() {
                    @Override
                    public LogEvent map(String s) throws Exception {
                        String[] tokens = s.toLowerCase().split("\\s+");
                        return LogEvent.of(tokens);
                    }
                }
        );

        operator = operator.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<LogEvent>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(LogEvent element) {
                        log.info("当前事件时间: {}, 水位线: {}", DateTimeUtils.epochMilli2Time(element.getEventTime()),
                                DateTimeUtils.epochMilli2Time(getCurrentWatermark().getTimestamp())
                        );
                        return element.getEventTime();
                    }
                }
        );

        operator.print("luvx");

        // 接收延迟事件
        OutputTag<LogEvent> tag = new OutputTag<LogEvent>("late") {
        };

        SingleOutputStreamOperator<Long> operator1 = operator
                .timeWindowAll(Time.seconds(10), Time.seconds(5))
                .allowedLateness(Time.seconds(10))
                .sideOutputLateData(tag)
                .apply(
                        new AllWindowFunction<LogEvent, Long, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window, Iterable<LogEvent> values, Collector<Long> out) throws Exception {
                                log.info("----------------- 窗口汇总↓ -----------------");
                                List<LogEvent> logs = new ArrayList<>();
                                Iterator<LogEvent> it = values.iterator();
                                while (it.hasNext()) {
                                    LogEvent log = it.next();
                                    logs.add(log);
                                }
                                log.info("{}个事件:{}", logs.size(), logs);
                                log.info("窗口时间:{} ~ {}", DateTimeUtils.epochMilli2Time(window.getStart()), DateTimeUtils.epochMilli2Time(window.getEnd()));
                                log.info("----------------- 窗口汇总↑ -----------------");

                                out.collect((long) logs.size());
                            }
                        }
                );
        DataStream<LogEvent> late = operator1.getSideOutput(tag);
        late.print("late");

        operator1.print("count");

        env.execute("window test example");
    }
}
