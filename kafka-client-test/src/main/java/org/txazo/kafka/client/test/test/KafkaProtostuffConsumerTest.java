package org.txazo.kafka.client.test.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;
import org.txazo.kafka.client.test.common.GsonUtil;
import org.txazo.kafka.client.test.common.KafkaBaseConsumer;
import org.txazo.kafka.client.test.common.PropertiesUtil;
import org.txazo.kafka.client.test.protostuff.CommonDeserializer;

import java.util.Properties;

/**
 * @author xiaozhou.tu
 */
public class KafkaProtostuffConsumerTest extends KafkaBaseConsumer {

    @Test
    public void testObject() {
        Properties properties = PropertiesUtil.getConsumerBaseProperties();
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CommonDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group-01");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consume(properties, "my-kafka-topic-test-006", false, (key, value) ->
                System.out.println("Consume: " + GsonUtil.toJsonString(value))
        );
    }

}
