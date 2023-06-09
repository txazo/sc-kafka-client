package org.txazo.kafka.client.test;

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
        produce(properties, "", 1000, 1, num ->
                new User()
        );
    }

}
