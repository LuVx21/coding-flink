package org.luvx.file;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.luvx.entity.UserBehaviorEvent;
import org.luvx.file.function.CountAgg;
import org.luvx.file.function.ItemViewCount;
import org.luvx.file.function.TopNHotItems;
import org.luvx.file.function.WindowResultFunction;

import java.io.File;
import java.net.URL;
import java.util.Objects;

/**
 * @ClassName: org.luvx.file
 * @Description:
 * @Author: Ren, Xie
 */
public class FileMain {
    private static final String LOCAL_LOCATION = "data/UserBehavior.csv";

    public static void main(String[] args) throws Exception {
        URL url = FileMain.class.getClassLoader().getResource(LOCAL_LOCATION);
        Path filePath = Path.fromLocalFile(new File(url.getPath()));
        PojoTypeInfo<UserBehaviorEvent> typeInfo = (PojoTypeInfo<UserBehaviorEvent>) TypeExtractor.createTypeInfo(UserBehaviorEvent.class);
        String[] columns = new String[]{"userId", "itemId", "categoryId", "behavior", "timestamp"};
        PojoCsvInputFormat<UserBehaviorEvent> format = new PojoCsvInputFormat<>(filePath, typeInfo, columns);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<UserBehaviorEvent> dataSource = env.createInput(format, typeInfo);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<UserBehaviorEvent> timeData = dataSource.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<UserBehaviorEvent>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehaviorEvent element) {
                        return element.getTs() * 1000;
                    }
                }
        );

        SingleOutputStreamOperator<UserBehaviorEvent> pvData = timeData.filter(
                new FilterFunction<UserBehaviorEvent>() {
                    @Override
                    public boolean filter(UserBehaviorEvent value) throws Exception {
                        return Objects.equals("pv", value.getBehavior());
                    }
                }
        );

        SingleOutputStreamOperator<ItemViewCount> windowData = pvData
                .keyBy("itemId")
                .timeWindow(Time.minutes(60), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResultFunction());

        SingleOutputStreamOperator<String> topItems = windowData
                .keyBy("windowEnd")
                .process(new TopNHotItems(4));

        topItems.print();

        env.execute("Test Hot Items Job");
    }
}
