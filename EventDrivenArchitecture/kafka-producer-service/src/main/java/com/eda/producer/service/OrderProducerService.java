package com.eda.producer.service;

import com.eda.producer.model.Order;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import java.util.concurrent.CompletableFuture;

@Service
public class OrderProducerService {

    @Autowired
    private KafkaTemplate<String, Order> kafkaTemplate;

    @Autowired
    @Qualifier("customSerializerKafkaTemplate")
    private KafkaTemplate<String, Order> customSerializerKafkaTemplate;


    public void sendWithKey(Order order) {
        String key = order.getOrderId();

        CompletableFuture<SendResult<String, Order>> future =
                kafkaTemplate.send("order-events", key, order);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Order event sent successfully!");
                System.out.println("Partition: " +  result.getRecordMetadata().partition());
                System.out.println("Offset: " + result.getRecordMetadata().offset());
            } else {
                System.out.println("Failed to send order event: " +  ex.getMessage());
            }
        });
    }

    public void sendWithNoKeyAndCustomSerializer(Order order) {

        // No key → partition chosen by sticky partitioner
        CompletableFuture<SendResult<String, Order>> future =
                customSerializerKafkaTemplate.send("order-events", order);

        //handle the callback
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Order event sent successfully!");
                System.out.println("Partition: " +  result.getRecordMetadata().partition());
                System.out.println("Offset: " + result.getRecordMetadata().offset());
            } else {
                System.out.println("Failed to send order event: " +  ex.getMessage());
            }
        });
    }
}



/*






public OrderProducerService(
            KafkaTemplate<String, Order> kafkaTemplate,
            @Qualifier("customSerializerKafkaTemplate") KafkaTemplate<String, Order> customSerializerKafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.customSerializerKafkaTemplate = customSerializerKafkaTemplate;
    }


    public void sendWithCustomSerializer(Order order) {
        String key = order.getOrderId();

        // Uses customSerializerKafkaTemplate which has OrderSummarySerializer
        CompletableFuture<SendResult<String, Order>> future =
                customSerializerKafkaTemplate.send(orderEventsTopic, key, order);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Order event sent successfully!");
                log.info("   Partition: {}", result.getRecordMetadata().partition());
                log.info("   Offset: {}", result.getRecordMetadata().offset());
            } else {
                log.error("{} Failed to send order event: {}", ex.getMessage());
            }
        });
    }
 */