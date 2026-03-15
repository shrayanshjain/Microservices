package com.eda.consumer.listener;

import com.eda.consumer.model.Order;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderEventListener {

    @KafkaListener(topics = "order-events")
    public void consume(Order order) {
        //business logic
        System.out.println("Received order event: " + order.getOrderId());
    }
}

