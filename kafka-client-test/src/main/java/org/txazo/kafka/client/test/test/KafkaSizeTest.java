package org.txazo.kafka.client.test.test;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.junit.Test;
import org.txazo.kafka.client.test.common.KafkaBaseProducerConsumer;
import org.txazo.kafka.client.test.common.PropertiesUtil;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @author xiaozhou.tu
 * @date 2023/6/14
 */
public class KafkaSizeTest extends KafkaBaseProducerConsumer {

    private static final String TOPIC = "topic-message-size-02";
    private static final String GROUP_ID = "my-consumer-group-39";

    @Test
    public void testProducer() throws Exception {
        String message = FileUtils.readFileToString(new File("/Users/xiaozhou.tu/Downloads/fullVpps_ES6.json"), StandardCharsets.UTF_8.name());
        Properties properties = PropertiesUtil.getProducerBaseProperties();
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 32 * 1024 * 1024L);
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 50 * 1024 * 1024);
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());
        produce(properties, TOPIC, 1, 1000, num -> message);
    }

    @Test
    public void testConsumer() {
        Properties properties = PropertiesUtil.getConsumerBaseProperties();
        properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 1024 * 1024);
        properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1024 * 1024);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consume(properties, Lists.newArrayList(TOPIC), false, (key, value) -> {
            System.out.println("Consume Big String size " + ((String) value).length());
        });
    }

}
