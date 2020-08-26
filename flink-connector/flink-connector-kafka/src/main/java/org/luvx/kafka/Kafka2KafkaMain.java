package org.luvx.kafka;

import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.luvx.common.utils.KafkaConfigUtils;
import org.luvx.common.utils.PropertiesUtils;
import org.luvx.entity.UserBehaviorEvent;
import org.luvx.kafka.schema.KafkaSchema;
import org.luvx.kafka.utils.KafkaUtils2;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @ClassName: org.luvx
 * @Description: 从kafka消费 -> sink到kafka
 * 反序列化: KafkaDeserializationSchema KafkaDeserializationSchemaWrapper
 * 序列化: KafkaSerializationSchema
 * @Author: Ren, Xie
 */
public class Kafka2KafkaMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<UserBehaviorEvent> source = env.addSource(new FlinkKafkaConsumer<>(
                KafkaUtils2.topic,
                // new KafkaSchema(),
                new KafkaDeserializationSchemaWrapper<>(new KafkaSchema()),
                KafkaConfigUtils.getConsumerProp()
        ));

        Properties pro0 = PropertiesUtils.getProperties("config/kafka/kafka.properties");
        String defaultTopic = (String) pro0.get("kafka.sink.topic");
        Properties pro1 = KafkaConfigUtils.getProducerProp();
        pro1.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        pro1.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        source.addSink(
                new FlinkKafkaProducer<>(
                        defaultTopic,
                        new KafkaSerializationSchema<UserBehaviorEvent>() {
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(UserBehaviorEvent element, @Nullable Long timestamp) {
                                return new ProducerRecord<>(defaultTopic, null, timestamp, null, JSON.toJSONString(element).getBytes());
                            }
                        },
                        pro1, FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                )
        ).name("flink-connector-kafka")
                .setParallelism(1);

        env.execute("from kafka");
    }
}
