# Spark Learn

### start zookeeper
``` sh
brew services start zookeeper
```

### start kafka
``` sh
brew services start kafka 
```

### create a topic
``` sh
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic test_topic
```

### package this example
``` sh
mvn clean package
```