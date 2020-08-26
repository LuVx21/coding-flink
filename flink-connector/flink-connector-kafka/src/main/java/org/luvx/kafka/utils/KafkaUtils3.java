package org.luvx.kafka.utils;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.luvx.common.utils.KafkaConfigUtils;
import org.luvx.entity.UserBehaviorEvent;
import org.luvx.entity.UserBehaviorEvent1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * @ClassName: org.luvx.kafka.utils
 * @Description:
 * @Author: Ren, Xie
 */
public class KafkaUtils3 {

    public static final String topic = "flink_table2";

    public static void main(String[] args) {
        try (InputStream inputStream = KafkaUtils3.class.getClassLoader().getResourceAsStream("data/UserBehavior.csv")) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                while (reader.ready()) {
                    String line = reader.readLine();
                    String[] tokens = line.split(",");
                    UserBehaviorEvent u = UserBehaviorEvent.of(tokens);
                    /// System.out.println(line);
                    send(UserBehaviorEvent1.of(u));
                    // send(u);
                    Thread.sleep(15_000);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void send(Object o) {
        Properties props = KafkaConfigUtils.getProducerProp();
        Producer<String, String> producer = new KafkaProducer<>(props);

        String msg = JSON.toJSONString(o);
        producer.send(new ProducerRecord<>(topic, null, null, msg));

        System.out.println("发送数据: " + msg);
        producer.flush();
    }
}
