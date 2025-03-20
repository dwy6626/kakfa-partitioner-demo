package com.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaProducerDemo {

    public static void main(String[] args) {
        System.out.println("\n=== Demo 1: default partitioner ===");
        runDemo("demo-topic-default", false, 4, null);

        System.out.println("\n=== Demo 2: customized partitioner ===");
        runDemo("demo-topic-custom", false, 4, "com.example.CustomPartitioner");

        System.out.println("\n=== Demo 3: partitioner.ignore.keys ===");
        runDemo("demo-topic-ignore-keys", true, 4, null);

        System.out.println("\n=== Demo 4: round robin partitioner ===");
        runDemo("demo-topic-roundrobin", false, 4, "org.apache.kafka.clients.producer.RoundRobinPartitioner");
    }

    private static void runDemo(String topic, boolean ignoreKeys, int numPartitions, String partitionerClass) {
        createTopicWithSpecifiedPartitionCount(topic, numPartitions);
        KafkaProducer<String, String> producer = getProducer(ignoreKeys, partitionerClass);

        VoteDataGenerator generator = new VoteDataGenerator();
        List<ProducerRecord<String, String>> votes = generator.generateVotes(topic, 100);

        Map<Integer, Integer> partitionCount = new HashMap<>();

        for (ProducerRecord<String, String> record : votes) {
            try {
                RecordMetadata metadata = producer.send(record).get();
                int partition = metadata.partition();
                partitionCount.put(partition, partitionCount.getOrDefault(partition, 0) + 1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        producer.close();

        System.out.println("----- " + topic + " Message Stats -----");
        for (Map.Entry<Integer, Integer> entry : partitionCount.entrySet()) {
            System.out.printf("Partition %d: %d messages%n", entry.getKey(), entry.getValue());
        }
    }

    private static KafkaProducer<String, String> getProducer(boolean ignoreKeys, String partitionerClass) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("batch.size", 100);

        // use interceptor to log result
        props.put("interceptor.classes", "com.example.LoggingInterceptor");
        if (partitionerClass != null && !partitionerClass.isEmpty()) {
            props.put("partitioner.class", partitionerClass);
        }
        if (ignoreKeys) {
            props.put("partitioner.ignore.keys", "true");
        }

      return new KafkaProducer<>(props);
    }

    private static void createTopicWithSpecifiedPartitionCount(String topic, int partitions) {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            // set replication factor to 1 because there is only 1 broker
            NewTopic newTopic = new NewTopic(topic, partitions, (short) 1);
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
            System.out.println("Topic " + topic + " created with " + partitions + " partitions.");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                System.out.println("Topic " + topic + " already exists.");
            } else {
                e.printStackTrace();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
