package com.eda.analytics.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

@Component
public class AnalyticsEventListener {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsEventListener.class);

    private final AtomicInteger totalOrders = new AtomicInteger(0);
    private final AtomicInteger totalRevenue = new AtomicInteger(0);

    /**
     * Listens to order-events topic
     * Consumer Group: analytics-service-group (DIFFERENT from notification-service)
     * 
     * This demonstrates:
     * - Multiple consumer groups reading same topic
     * - Each group maintains its own offset
     * - Auto-commit for at-most-once delivery (acceptable for analytics)
     */
    @KafkaListener(
        topics = "${kafka.topics.order-events}",
        groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consumeOrderEventForAnalytics(ConsumerRecord<String, String> record) {
        try {
            log.info("📊 Analytics: Received order event");
            log.info("   Partition: {}, Offset: {}, Key: {}", 
                record.partition(), record.offset(), record.key());

            // Process for analytics
            processAnalytics(record.value());

        } catch (Exception e) {
            log.error("❌ Analytics: Error processing event: {}", e.getMessage());
            // With auto-commit, offset is committed even if processing fails
            // This is acceptable for analytics (at-most-once delivery)
        }
    }

    /**
     * Process order data for analytics
     */
    private void processAnalytics(String orderJson) {
        int count = totalOrders.incrementAndGet();
        
        // Extract amount from JSON (simplified)
        try {
            String amountStr = orderJson.split("\"totalAmount\":")[1].split(",")[0];
            double amount = Double.parseDouble(amountStr);
            totalRevenue.addAndGet((int) amount);
        } catch (Exception e) {
            log.warn("Could not parse amount from order");
        }

        log.info("📈 Analytics Update:");
        log.info("   Total Orders Processed: {}", count);
        log.info("   Total Revenue: ${}", totalRevenue.get());

        // In real scenario: store in database, update dashboards, etc.
    }
}

