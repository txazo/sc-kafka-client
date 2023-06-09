package org.txazo.kafka.client.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;
import org.txazo.kafka.client.test.common.KafkaBaseConsumer;
import org.txazo.kafka.client.test.common.PropertiesUtil;

import java.util.Properties;

/**
 * @author xiaozhou.tu
 */
public class KafkaConsumerTest extends KafkaBaseConsumer {

    @Test
    public void test01() {
        Properties properties = PropertiesUtil.getConsumerBaseProperties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
        consume(properties, "");
    }

}
