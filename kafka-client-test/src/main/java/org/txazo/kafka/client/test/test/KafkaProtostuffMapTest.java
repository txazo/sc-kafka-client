package org.txazo.kafka.client.test.test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;
import org.txazo.kafka.client.test.common.GsonUtil;
import org.txazo.kafka.client.test.common.KafkaBaseProducerConsumer;
import org.txazo.kafka.client.test.common.PropertiesUtil;
import org.txazo.kafka.client.test.protostuff.CommonDeserializer;
import org.txazo.kafka.client.test.protostuff.CommonSerializer;

import java.util.Date;
import java.util.Properties;

/**
 * @author xiaozhou.tu
 */
public class KafkaProtostuffMapTest extends KafkaBaseProducerConsumer {

    private static final String TOPIC = "my-kafka-topic-test-010";
    private static final String GROUP_ID = "my-consumer-group-01";

    @Test
    public void testProducer() {
        Properties properties = PropertiesUtil.getProducerBaseProperties();
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CommonSerializer.class.getName());
        produce(properties, TOPIC, 1000, 2000, num -> Maps.newHashMap(ImmutableMap.of(
                "id", num,
                "userName", RandomStringUtils.randomAlphabetic(10),
                "age", RandomUtils.nextInt(0, 100),
                "createTime", new Date(),
                "updateTime", new Date()
        )));
    }

    @Test
    public void testConsumer() {
        Properties properties = PropertiesUtil.getConsumerBaseProperties();
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CommonDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consume(properties, TOPIC, false, (key, value) ->
                System.out.println("Consume Protostuff Map: " + GsonUtil.toJsonString(value))
        );
    }

}
