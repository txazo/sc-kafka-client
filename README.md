
```shell
bin/kafka-topics.sh --create --topic my-kafka-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
bin/kafka-topics.sh --create --topic my-kafka-topic-test-013 --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1
bin/kafka-topics.sh --create --topic topic-message-size-01 --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
bin/kafka-topics.sh --create --topic topic-message-size-02 --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

bin/kafka-topics.sh --delete --topic my-kafka-topic-test-007 --bootstrap-server localhost:9092

bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe

bin/kafka-configs.sh --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name 'topic-message-size-01' --add-config 'max.message.bytes=524288000'
```

```shell
~/Software/kafka/kafka_2.13-3.4.0/bin/kafka-dump-log.sh --files 00000000000000000000.log
```
