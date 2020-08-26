package org.luvx.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.luvx.common.utils.KafkaConfigUtils;
import org.luvx.entity.UserBehaviorEvent;
import org.luvx.kafka.utils.KafkaUtils2;

import java.util.Objects;

/**
 * @ClassName: org.luvx
 * @Description:
 * @Author: Ren, Xie
 */
public class Main {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<>(
                KafkaUtils2.topic,
                new SimpleStringSchema(),
                KafkaConfigUtils.getConsumerProp()
        ));

        SingleOutputStreamOperator<UserBehaviorEvent> operator = map(source);

        // map1(operator);
        // flatMap(operator);
        // filter(operator);
        // keyBy(operator);

        /// source.print("kafka");
        env.execute("from kafka");
    }

    private static SingleOutputStreamOperator<UserBehaviorEvent> map(DataStreamSource<String> stream) {
        SingleOutputStreamOperator<UserBehaviorEvent> operator = stream.map(
                s -> JSON.parseObject(s, UserBehaviorEvent.class)
        );
        return operator;
    }

    private static void map1(SingleOutputStreamOperator<UserBehaviorEvent> stream) {
        SingleOutputStreamOperator<UserBehaviorEvent> operator = stream.map(
                s -> {
                    UserBehaviorEvent u = UserBehaviorEvent.of(s);
                    u.setTs(s.getTs() / 1000);
                    return u;
                }
        );
        operator.print();
    }

    private static void flatMap(SingleOutputStreamOperator<UserBehaviorEvent> operator) {
        SingleOutputStreamOperator<UserBehaviorEvent> temp = operator.flatMap(
                new FlatMapFunction<UserBehaviorEvent, UserBehaviorEvent>() {
                    @Override
                    public void flatMap(UserBehaviorEvent userBehavior, Collector<UserBehaviorEvent> collector) throws Exception {
                        if (userBehavior.getUserId() % 2 == 0) {
                            collector.collect(userBehavior);
                        }
                    }
                }
        );
        temp.print();
    }

    private static void filter(SingleOutputStreamOperator<UserBehaviorEvent> operator) {
        SingleOutputStreamOperator<UserBehaviorEvent> temp = operator.filter(
                userBehavior -> Objects.equals(userBehavior.getBehavior(), "pv")
        );
        temp.print();
    }

    private static void keyBy(SingleOutputStreamOperator<UserBehaviorEvent> operator) {
        KeyedStream<UserBehaviorEvent, String> stream1 = operator.keyBy(
                new KeySelector<UserBehaviorEvent, String>() {
                    @Override
                    public String getKey(UserBehaviorEvent userBehavior) throws Exception {
                        return userBehavior.getBehavior();
                    }
                }
        );
        SingleOutputStreamOperator<UserBehaviorEvent> stream2 = stream1.reduce(
                new ReduceFunction<UserBehaviorEvent>() {
                    @Override
                    public UserBehaviorEvent reduce(UserBehaviorEvent t, UserBehaviorEvent t1) throws Exception {
                        return UserBehaviorEvent.builder()
                                .userId(t.getUserId() + t1.getUserId())
                                .build();
                    }
                });

        stream2.print();
    }
}
