package org.luvx.common.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.Properties;
import java.util.Set;

/**
 * @ClassName: org.luvx.common.utils
 * @Description:
 * @Author: Ren, Xie
 */
@Slf4j
public class KafkaConfigUtils {

    public static Properties getProducerPropNoSerializer() {
        Properties pro = getProducerProp();
        pro.remove("key.serializer");
        pro.remove("value.serializer");
        return pro;
    }

    public static Properties getProducerProp() {
        return PropertiesUtils.getProperties("config/kafka/kafka-producer.properties");
    }

    public static Properties getConsumerProp() {
        return PropertiesUtils.getProperties("config/kafka/kafka-consumer.properties");
    }
}
