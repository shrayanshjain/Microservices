package com.eda.consumer.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    /**
     * Listens to order-events topic
     * Consumer Group: notification-service-group
     * 
     * This demonstrates:
     * - Consumer reading from partition
     * - Manual offset commit (at-least-once delivery)
     * - Processing events
     */
    @KafkaListener(
        topics = "${kafka.topics.order-events}",
        groupId = "${spring.kafka.consumer.group-id}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderEvent(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        try {
            log.info("📨 Received order event from Kafka");
            log.info("   Topic: {}", record.topic());
            log.info("   Partition: {}", record.partition());
            log.info("   Offset: {}", record.offset());
            log.info("   Key: {}", record.key());
            log.info("   Timestamp: {}", record.timestamp());
            log.debug("   Value: {}", record.value());

            // Simulate processing the order event
            processOrderEvent(record.key(), record.value());

            // Manual commit after successful processing (at-least-once delivery)
            acknowledgment.acknowledge();
            log.info("✅ Order event processed and offset committed");

        } catch (Exception e) {
            log.error("❌ Error processing order event: {}", e.getMessage());
            // Don't commit offset - message will be reprocessed
            throw new RuntimeException("Failed to process order event", e);
        }
    }

    /**
     * Simulates processing the order event
     * In real scenario: send email, SMS, push notification, etc.
     */
    private void processOrderEvent(String orderId, String orderJson) {
        log.info("🔔 Sending notification for order: {}", orderId);
        
        // Simulate some processing time
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        log.info("📧 Email sent to customer for order: {}", orderId);
        log.info("📱 SMS sent to customer for order: {}", orderId);
    }
}

