package com.eda.producer.controller;

import com.eda.producer.model.Order;
import com.eda.producer.service.OrderProducerService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    @Autowired
    OrderProducerService orderProducerService;


    @PostMapping("/with-key")
    public ResponseEntity<String> sendWithKey(@RequestBody Order order) {
        orderProducerService.sendWithKey(order);
        return ResponseEntity.accepted().body("Order created and event published!");
    }

    @PostMapping("/with-no-key")
    public ResponseEntity<String> sendWithNoKey(@RequestBody Order order) {
        orderProducerService.sendWithNoKeyAndCustomSerializer(order);
        return ResponseEntity.accepted().body("Order created and event published!");
    }
}