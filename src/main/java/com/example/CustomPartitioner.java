package com.example;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    @Override
    public void configure(Map<String, ?> configs) {
        // customized configuration here
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int numPartitions = cluster.partitionCountForTopic(topic);

        // If no key is provided, randomly assign to any partition
        if (keyBytes == null || !(key instanceof String)) {
            return (int) (Math.random() * numPartitions);
        }

        String keyStr = (String) key;
        if ("Benjamin".equals(keyStr)) {
            return 0;
        }

        // Other keys randomly assigned to partitions 1 to (numPartitions - 1)
        return 1 + (int) (Math.random() * (numPartitions - 1));
    }

    @Override
    public void close() {
        // release resource if needed
    }
}
