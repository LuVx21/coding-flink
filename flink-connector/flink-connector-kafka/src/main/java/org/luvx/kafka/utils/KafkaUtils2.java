package org.luvx.kafka.utils;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.luvx.entity.UserBehaviorEvent;
import org.luvx.common.utils.KafkaConfigUtils;

import java.util.Properties;
import java.util.Random;

/**
 * 造数据用(send to kafka)
 */
public class KafkaUtils2 {

    public static final String topic  = "flink";
    private static      long   userId = 1000L;

    public static void main(String[] args) throws InterruptedException {
        for (; ; ) {
            Thread.sleep(15_000);
            send();
        }
    }

    private static void send() {
        Properties props = KafkaConfigUtils.getProducerProp();
        Producer<String, String> producer = new KafkaProducer<>(props);

        UserBehaviorEvent user = make();
        String msg = JSON.toJSONString(user);
        producer.send(new ProducerRecord<>(topic, null, null, msg));

        System.out.println("发送数据: " + msg);
        producer.flush();
    }

    private static UserBehaviorEvent make() {
        String[] a = {"pv", "buy", "cart", "fav"};
        // [0, 3]
        Random r = new Random();
        int i = r.nextInt(4) % (4);

        return UserBehaviorEvent.builder()
                .userId(userId++)
                .itemId(2001L)
                .categoryId(101)
                .behavior(a[i])
                .ts(System.currentTimeMillis())
                .build();
    }
}
