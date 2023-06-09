package org.txazo.kafka.client.test;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;
import org.txazo.kafka.client.test.bean.User;
import org.txazo.kafka.client.test.common.KafkaBaseProducer;
import org.txazo.kafka.client.test.common.PropertiesUtil;

import java.util.*;

/**
 * @author xiaozhou.tu
 */
public class KafkaProducerTest extends KafkaBaseProducer {

    @Test
    public void test01() {
        Properties properties = PropertiesUtil.getProducerBaseProperties();
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(1024 * 1024));
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.apache.kafka.clients.producer.RoundRobinPartitioner");

        produce(properties, "", 1000, 1, num ->
                new User()
        );
    }

}
