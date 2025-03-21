package com.example;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.Map;

public class LoggingInterceptor implements ProducerInterceptor<String, String> {

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        System.out.printf("[Interceptor] onSend: key=%s, value=%s%n", record.key(), record.value());
        record.headers().add("example-header", "example-content".getBytes());
        return record;
    }
    
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            System.out.printf("[Interceptor] onAcknowledgement: partition=%d, offset=%d%n",
                              metadata.partition(), metadata.offset());
        } else {
            System.out.println("[Interceptor] onAcknowledgement encountered exception: " + exception);
        }
    }
    
    @Override
    public void close() {
        // resource cleanup
        System.out.println("[Interceptor] closing interceptor");
    }
    
    @Override
    public void configure(Map<String, ?> configs) {
        // configure interceptor
        System.out.println("configuring LoggingInterceptor with producer config: " + configs);
    }
}
