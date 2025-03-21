package com.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import com.example.Vote;

public class KafkaProducerDemo {

    private static String brokerUrl = "kafka:9092";

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java -jar kafka-producer-demo.jar <demoNumber>");
            System.out.println("Available demo numbers: 1 - default partitioner");
            System.out.println("                       2 - customized partitioner");
            System.out.println("                       3 - partitioner.ignore.keys");
            System.out.println("                       4 - round robin partitioner");
            return;
        }

        String demoNumber = args[0];

        switch (demoNumber) {
            case "1":
                System.out.println("\n=== Demo 1: default partitioner ===");
                runDemo("demo-topic-default", false, 4, null);
                break;
            case "2":
                System.out.println("\n=== Demo 2: customized partitioner ===");
                runDemo("demo-topic-custom", false, 4, "com.example.CustomPartitioner");
                break;
            case "3":
                System.out.println("\n=== Demo 3: partitioner.ignore.keys ===");
                runDemo("demo-topic-ignore-keys", true, 4, null);
                break;
            case "4":
                System.out.println("\n=== Demo 4: round robin partitioner ===");
                runDemo("demo-topic-roundrobin", false, 4, "org.apache.kafka.clients.producer.RoundRobinPartitioner");
                break;
            default:
                System.out.println("Invalid demo number: " + demoNumber);
                System.out.println("Available demo numbers: 1 - default partitioner");
                System.out.println("                       2 - customized partitioner");
                System.out.println("                       3 - partitioner.ignore.keys");
                System.out.println("                       4 - round robin partitioner");
        }
    }

    private static void runDemo(String topic, boolean ignoreKeys, int numPartitions, String partitionerClass) {
        createTopicWithSpecifiedPartitionCount(topic, numPartitions);
        KafkaProducer<String, Vote> producer = getProducer(ignoreKeys, partitionerClass);

        VoteDataGenerator generator = new VoteDataGenerator();
        List<ProducerRecord<String, Vote>> votes = generator.generateVotes(topic, 100);

        Map<Integer, Integer> partitionCount = new HashMap<>();

        for (ProducerRecord<String, Vote> record : votes) {
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

    private static KafkaProducer<String, Vote> getProducer(boolean ignoreKeys, String partitionerClass) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerUrl);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", "http://schema-registry:8081");

        // load batch size from config yaml
        Map<String, Object> yamlConfig = loadYamlConfig();
        Object batchSize = yamlConfig.get("batch.size");
        if (batchSize instanceof Number) {
            props.put("batch.size", ((Number) batchSize).intValue());
        }

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
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);

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

    private static Map<String, Object> loadYamlConfig() {
        org.yaml.snakeyaml.Yaml yaml = new org.yaml.snakeyaml.Yaml();
        Map<String, Object> yamlConfig = new HashMap<>();
        try (java.io.InputStream inputStream = new java.io.FileInputStream("./configuration/producer-config.yaml")) {
            yamlConfig = yaml.load(inputStream);
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
        return yamlConfig != null ? yamlConfig : new HashMap<>();
    }
}
