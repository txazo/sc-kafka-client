package org.txazo.kafka.client.test.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;
import org.txazo.kafka.client.test.common.GsonUtil;
import org.txazo.kafka.client.test.common.KafkaBaseProducerConsumer;
import org.txazo.kafka.client.test.common.PropertiesUtil;
import org.txazo.kafka.client.test.protostuff.CommonDeserializer;
import org.txazo.kafka.client.test.protostuff.CommonSerializer;

import java.util.Properties;

/**
 * @author xiaozhou.tu
 */
public class KafkaProtostuffListTest extends KafkaBaseProducerConsumer {

    private static final String TOPIC = "my-kafka-topic-test-008";
    private static final String GROUP_ID = "my-consumer-group-01";

    @Test
    public void testProducer() {
        Properties properties = PropertiesUtil.getProducerBaseProperties();
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CommonSerializer.class.getName());
        produce(properties, TOPIC, 1000, 2000, this::newRandomUserList);
    }

    @Test
    public void testConsumer() {
        Properties properties = PropertiesUtil.getConsumerBaseProperties();
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CommonDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consume(properties, TOPIC, false, (key, value) ->
                System.out.println("Consume Protostuff List: " + GsonUtil.toJsonString(value))
        );
    }

}
