package org.luvx.kafka.schema;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.luvx.entity.UserBehaviorEvent;

import java.io.IOException;

/**
 * @ClassName: org.luvx.kafka.schema
 * @Description:
 * @Author: Ren, Xie
 */
public class KafkaSchema implements DeserializationSchema<UserBehaviorEvent>, SerializationSchema<UserBehaviorEvent> {
    @Override
    public UserBehaviorEvent deserialize(byte[] message) throws IOException {
        return JSON.parseObject(message, UserBehaviorEvent.class);
    }

    @Override
    public boolean isEndOfStream(UserBehaviorEvent nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(UserBehaviorEvent element) {
        return JSON.toJSONString(element).getBytes();
    }

    @Override
    public TypeInformation<UserBehaviorEvent> getProducedType() {
        return TypeInformation.of(UserBehaviorEvent.class);
    }
}
