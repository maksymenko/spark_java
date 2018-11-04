## Quick start

#### Resources
* https://github.com/jleetutorial/sparkTutorial

----


## Setup kafka spark pipeline

### Download distributives
```
https://spark.apache.org/
https://kafka.apache.org
```

### Use **kafka connect** for publisher and consumer

* Start Server
```
 $ bin/zookeeper-server-start.sh config/zookeeper.properties
 $ bin/kafka-server-start.sh config/server.properties
```
* Create topic
```
$ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic first_test_topic
$ bin/kafka-topics.sh --list --zookeeper localhost:2181
```
* Start consumer
```
$ bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_test_topic --from-beginning
```
* Start message publisher
```
$ bin/kafka-console-producer.sh --broker-list localhost:9092 --topic first_test_topic
type: Hello
```

* Consume by spark `kafka_consume.Handler`

