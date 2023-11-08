package org.txazo.kafka.client.test.test;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.StickyAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
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

    private static final String TOPIC = "topic-A";
    private static final String TOPIC_2 = "topic-B";
    private static final String GROUP_ID = "my-consumer-group-41";

    @Test
    public void testProducer() {
        Properties properties = PropertiesUtil.getProducerBaseProperties();
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CommonSerializer.class.getName());
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());
        produce(properties, TOPIC_2, 100000, 1, num -> GsonUtil.toJsonString(newRandomUser(num)));
    }

    @Test
    public void testConsumer() {
        Properties properties = PropertiesUtil.getConsumerBaseProperties();
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CommonDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consume(properties, Lists.newArrayList(TOPIC, TOPIC_2), false, (key, value) -> {
            User user = GsonUtil.parseJson((String) value, User.class);
            System.out.println("Consume String: " + GsonUtil.toJsonString(user));
        });
    }

    @Test
    public void testMultiConsumer() throws Exception {
        int size = 2;
        Properties properties = PropertiesUtil.getConsumerBaseProperties();
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CommonDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                Lists.newArrayList(StickyAssignor.class.getName()));
        multiConsume(size, properties, Lists.newArrayList(TOPIC, TOPIC_2), false, (key, value) -> {
            User user = GsonUtil.parseJson((String) value, User.class);
            System.out.println("Consume String: " + GsonUtil.toJsonString(user));
        });
        Thread.sleep(1000 * 60 * 5);
        consume(properties, Lists.newArrayList(TOPIC, TOPIC_2), false, (key, value) -> {
            User user = GsonUtil.parseJson((String) value, User.class);
            System.out.println("Consume String: " + GsonUtil.toJsonString(user));
        });
        Thread.sleep(MAX_TIME);
    }

}
