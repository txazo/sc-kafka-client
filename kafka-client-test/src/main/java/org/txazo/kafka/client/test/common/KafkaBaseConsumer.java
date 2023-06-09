package org.txazo.kafka.client.test.common;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;
import java.util.function.BiConsumer;

/**
 * @author xiaozhou.tu
 * @date 2023/6/9
 */
public class KafkaBaseConsumer {

    private static final int MAX_TIME = 1000 * 60 * 60;

    public void consume(Properties properties, String topic, boolean log, BiConsumer<String, Object> keyValueCallback) {
        long startTime = System.currentTimeMillis();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            while (System.currentTimeMillis() - startTime < MAX_TIME) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));
                if (consumerRecords != null && !consumerRecords.isEmpty()) {
                    for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                        if (log) {
                            System.out.printf("Topic: %s Partition: %d Offset: %d Key: %s Value: %s %n",
                                    consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
                                    consumerRecord.key(), consumerRecord.value());
                        }
                        keyValueCallback.accept(consumerRecord.key(), consumerRecord.value());
                    }
                    consumer.commitSync();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
