package com.eda.consumer.listener;

import com.eda.consumer.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * MINIMAL consumer — just @KafkaListener + a method parameter.
 *
 * What happens behind the scenes (all automatic):
 *   1. Spring sees @KafkaListener → creates a KafkaMessageListenerContainer
 *   2. Container creates a KafkaConsumer with your application.properties config
 *   3. KafkaConsumer connects to bootstrap broker, finds Group Coordinator
 *   4. Joins consumer group "order-consumer-group", gets partition assignment
 *   5. Starts poll() loop → fetches records → calls this method → auto-commits offsets
 *
 * No @Configuration class needed. No ConsumerFactory bean. No ContainerFactory bean.
 * Spring Boot auto-configures ALL of it from application.properties.
 */
@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    /**
     * topics = which Kafka topic to subscribe to
     *
     * Spring's JsonDeserializer sees the method parameter type (Order)
     * and deserializes the JSON bytes into an Order object.
     *
     * BUT WAIT — the producer's __TypeId__ header says "com.eda.producer.model.Order"
     * and we have "com.eda.consumer.model.Order" — different package!
     *
     * This would normally cause ClassNotFoundException.
     * We need ONE of these fixes in application.properties:
     *   Option A: spring.json.type.mapping=com.eda.producer.model.Order:com.eda.consumer.model.Order
     *   Option B: spring.json.use.type.headers=false (ignore header, infer type from method param)
     *
     * For UseCase 1 (minimal), we'll use Option A (explicit mapping) since it's
     * the most straightforward — you see exactly what's happening.
     */
    @KafkaListener(topics = "order-events")
    public void consume(Order order) {
        log.info("✅ Received order: {}", order);
    }
}

