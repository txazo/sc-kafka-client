package org.txazo.kafka.client.test.test;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.junit.Test;
import org.txazo.kafka.client.test.common.GsonUtil;
import org.txazo.kafka.client.test.common.KafkaBaseProducerConsumer;
import org.txazo.kafka.client.test.common.PropertiesUtil;

import java.util.Date;
import java.util.Properties;

/**
 * @author xiaozhou.tu
 * @date 2023/6/9
 */
public class RoundRobinPartitionerTest extends KafkaBaseProducerConsumer {

    private static final String TOPIC = "my-kafka-topic-test-000";

    /**
     * bin/kafka-topics.sh --create --topic my-kafka-topic-test-000 --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1
     */
    @Test
    public void testProducer() {
        Properties properties = PropertiesUtil.getProducerBaseProperties();
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());
        produce(properties, TOPIC, 100000, 0, num -> GsonUtil.toJsonString(ImmutableMap.of(
                "id", num,
                "userName", RandomStringUtils.randomAlphabetic(10),
                "age", RandomUtils.nextInt(0, 100),
                "createTime", new Date(),
                "updateTime", new Date()
        )));
    }

}
