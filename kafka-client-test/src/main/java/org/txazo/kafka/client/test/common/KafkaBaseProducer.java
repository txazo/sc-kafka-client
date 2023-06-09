package org.txazo.kafka.client.test.common;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.function.Function;

/**
 * @author xiaozhou.tu
 * @date 2023/6/9
 */
public class KafkaBaseProducer {

    public void produce(Properties properties, String topic, int size, long sleep, Function<Integer, Object> valueFunction) {
        try (KafkaProducer<String, Object> kafkaProducer = new KafkaProducer<>(properties)) {
            for (int i = 1; i <= size; i++) {
                final int num = i;
                ProducerRecord<String, Object> record = new ProducerRecord<>(topic, valueFunction.apply(num));
                kafkaProducer.send(record, (metadata, exception) ->
                        System.out.println("Kafka callback " + num + " metadata: " + metadata.partition()
                                + " " + metadata.offset() + " exception: " + exception)
                );
                Thread.sleep(sleep);
            }
            Thread.sleep(1000 * 60 * 60);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
