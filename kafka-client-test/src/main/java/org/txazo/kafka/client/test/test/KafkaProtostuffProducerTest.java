package org.txazo.kafka.client.test.test;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;
import org.txazo.kafka.client.test.bean.User;
import org.txazo.kafka.client.test.common.KafkaBaseProducer;
import org.txazo.kafka.client.test.common.PropertiesUtil;
import org.txazo.kafka.client.test.protostuff.CommonSerializer;

import java.util.Date;
import java.util.Properties;

/**
 * @author xiaozhou.tu
 * @date 2023/6/9
 */
public class KafkaProtostuffProducerTest extends KafkaBaseProducer {

    @Test
    public void testObject() {
        Properties properties = PropertiesUtil.getProducerBaseProperties();
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CommonSerializer.class.getName());
        produce(properties, "my-kafka-topic-test-006", 100, 2000, num ->
                new User((long) num, RandomStringUtils.randomAlphabetic(10),
                        RandomUtils.nextInt(0, 100), new Date(), new Date())
        );
    }

}
