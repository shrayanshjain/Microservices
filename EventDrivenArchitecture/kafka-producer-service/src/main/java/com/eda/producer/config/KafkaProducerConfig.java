package com.eda.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaProducerConfig {

    @Value("${kafka.topics.order-events}")
    private String orderEventsTopic;

    @Value("${kafka.topics.payment-events}")
    private String paymentEventsTopic;

    @Value("${kafka.partitions}")
    private int partitions;

    @Value("${kafka.replication-factor}")
    private int replicationFactor;

    /**
     * Create order-events topic with 3 partitions
     * This demonstrates topic creation from application
     */
    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name(orderEventsTopic)
                .partitions(partitions)
                .replicas(replicationFactor)
                .build();
    }

    /**
     * Create payment-events topic with 3 partitions
     */
    @Bean
    public NewTopic paymentEventsTopic() {
        return TopicBuilder.name(paymentEventsTopic)
                .partitions(partitions)
                .replicas(replicationFactor)
                .build();
    }
}

