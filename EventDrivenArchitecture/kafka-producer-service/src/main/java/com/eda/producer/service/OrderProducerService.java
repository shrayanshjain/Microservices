package com.eda.producer.service;

import com.eda.producer.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class OrderProducerService {

    private static final Logger log = LoggerFactory.getLogger(OrderProducerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topics.order-events}")
    private String orderEventsTopic;

    public OrderProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Produces order event to Kafka
     * Key = orderId (ensures all events for same order go to same partition)
     * Value = order JSON
     */
    public void sendOrderEvent(Order order) {
        String key = order.getOrderId();
        String value = order.toJson();

        log.info("📤 Sending order event - Key: {}, Topic: {}", key, orderEventsTopic);
        log.debug("Event payload: {}", value);

        // Send to Kafka asynchronously
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(orderEventsTopic, key, value);

        // Handle success/failure
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("✅ Order event sent successfully!");
                log.info("   Topic: {}", result.getRecordMetadata().topic());
                log.info("   Partition: {}", result.getRecordMetadata().partition());
                log.info("   Offset: {}", result.getRecordMetadata().offset());
                log.info("   Timestamp: {}", result.getRecordMetadata().timestamp());
            } else {
                log.error("❌ Failed to send order event: {}", ex.getMessage());
            }
        });
    }

    /**
     * Synchronous send - waits for acknowledgment
     */
    public void sendOrderEventSync(Order order) {
        String key = order.getOrderId();
        String value = order.toJson();

        log.info("📤 Sending order event (SYNC) - Key: {}", key);

        try {
            SendResult<String, String> result = kafkaTemplate.send(orderEventsTopic, key, value).get();
            log.info("✅ Order event sent successfully (SYNC)!");
            log.info("   Partition: {}, Offset: {}", 
                result.getRecordMetadata().partition(), 
                result.getRecordMetadata().offset());
        } catch (Exception e) {
            log.error("❌ Failed to send order event (SYNC): {}", e.getMessage());
            throw new RuntimeException("Failed to send order event", e);
        }
    }
}

