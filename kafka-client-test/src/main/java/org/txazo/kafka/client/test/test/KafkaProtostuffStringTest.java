package org.txazo.kafka.client.test.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;
import org.txazo.kafka.client.test.bean.User;
import org.txazo.kafka.client.test.common.GsonUtil;
import org.txazo.kafka.client.test.common.KafkaBaseProducerConsumer;
import org.txazo.kafka.client.test.common.PropertiesUtil;
import org.txazo.kafka.client.test.protostuff.CommonDeserializer;
import org.txazo.kafka.client.test.protostuff.CommonSerializer;

import java.util.Properties;

/**
 * @author xiaozhou.tu
 */
public class KafkaProtostuffStringTest extends KafkaBaseProducerConsumer {

    private static final String TOPIC = "my-kafka-topic-test-007";
    private static final String GROUP_ID = "my-consumer-group-01";

    @Test
    public void testProducer() {
        Properties properties = PropertiesUtil.getProducerBaseProperties();
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CommonSerializer.class.getName());
        produce(properties, TOPIC, 1000, 2000, num -> GsonUtil.toJsonString(newRandomUser(num)));
    }

    @Test
    public void testConsumer() {
        Properties properties = PropertiesUtil.getConsumerBaseProperties();
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CommonDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consume(properties, TOPIC, false, (key, value) -> {
            User user = GsonUtil.parseJson((String) value, User.class);
            System.out.println("Consume String: " + GsonUtil.toJsonString(user));
        });
    }

}
