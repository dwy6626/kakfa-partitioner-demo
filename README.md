# Kafka Partitioner Demo

- Producer: written in Java and built by Maven / Docker
- Kafka: use docker-compose to setup

```bash
# (optional) build project locally to get the generated Avro class e.g. Vote.java
mvn build

# Build the demo producer:
docker build . -t kafka-demo

# start kafka with KRaft
docker-compose up

# run demo producer
./run-producer.sh 1  # default partitioner
./run-producer.sh 2  # customized partitioner
...
```
