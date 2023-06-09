package org.txazo.kafka.client.test.common;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.txazo.kafka.client.test.bean.User;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * @author xiaozhou.tu
 * @date 2023/6/9
 */
public class KafkaBaseProducerConsumer {

    protected static final int MAX_TIME = 1000 * 60 * 60;

    public void produce(Properties properties, String topic, int size, long sleep, Function<Integer, Object> valueFunction) {
        try (KafkaProducer<String, Object> kafkaProducer = new KafkaProducer<>(properties)) {
            for (int i = 1; i <= size; i++) {
                final int num = i;
                ProducerRecord<String, Object> record = new ProducerRecord<>(topic, valueFunction.apply(num));
                kafkaProducer.send(record, (metadata, exception) ->
                        System.out.println("Kafka callback " + num + " partition: " + metadata.partition()
                                + " offset: " + metadata.offset() + " exception: " + exception)
                );
                if (sleep > 0) {
                    Thread.sleep(sleep);
                }
            }
            Thread.sleep(MAX_TIME);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

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

    protected User newRandomUser(int num) {
        return new User((long) num, RandomStringUtils.randomAlphabetic(10), RandomUtils.nextInt(0, 100), new Date(), new Date());
    }

    protected List<User> newRandomUserList(int num) {
        List<User> userList = Lists.newArrayList();
        int size = RandomUtils.nextInt(1, 10);
        for (int i = 0; i < size; i++) {
            userList.add(newRandomUser(num));
        }
        return userList;
    }

}
