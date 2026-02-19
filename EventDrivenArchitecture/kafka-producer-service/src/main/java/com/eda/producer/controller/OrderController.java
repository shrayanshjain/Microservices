package com.eda.producer.controller;

import com.eda.producer.model.Order;
import com.eda.producer.service.OrderProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private final OrderProducerService orderProducerService;

    public OrderController(OrderProducerService orderProducerService) {
        this.orderProducerService = orderProducerService;
    }

    /**
     * Create a new order and publish to Kafka
     * POST http://localhost:8081/api/orders
     */
    @PostMapping
    public ResponseEntity<Map<String, String>> createOrder(@RequestBody Order order) {
        // Generate order ID if not provided
        if (order.getOrderId() == null || order.getOrderId().isEmpty()) {
            order.setOrderId("ORDER-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase());
        }
        
        order.setCreatedAt(LocalDateTime.now());
        order.setStatus("CREATED");

        log.info("🛒 Creating order: {}", order.getOrderId());

        // Send to Kafka
        orderProducerService.sendOrderEvent(order);

        Map<String, String> response = new HashMap<>();
        response.put("orderId", order.getOrderId());
        response.put("status", "Order event published to Kafka");
        response.put("message", "Order will be processed asynchronously");

        return ResponseEntity.accepted().body(response);
    }

    /**
     * Create order with synchronous Kafka send
     * POST http://localhost:8081/api/orders/sync
     */
    @PostMapping("/sync")
    public ResponseEntity<Map<String, String>> createOrderSync(@RequestBody Order order) {
        if (order.getOrderId() == null || order.getOrderId().isEmpty()) {
            order.setOrderId("ORDER-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase());
        }
        
        order.setCreatedAt(LocalDateTime.now());
        order.setStatus("CREATED");

        log.info("🛒 Creating order (SYNC): {}", order.getOrderId());

        // Send to Kafka synchronously
        orderProducerService.sendOrderEventSync(order);

        Map<String, String> response = new HashMap<>();
        response.put("orderId", order.getOrderId());
        response.put("status", "Order event published to Kafka (confirmed)");

        return ResponseEntity.ok(response);
    }

    /**
     * Bulk create orders for testing
     * POST http://localhost:8081/api/orders/bulk?count=10
     */
    @PostMapping("/bulk")
    public ResponseEntity<Map<String, Object>> createBulkOrders(@RequestParam(defaultValue = "10") int count) {
        log.info("🛒 Creating {} orders in bulk", count);

        for (int i = 0; i < count; i++) {
            Order order = new Order(
                "ORDER-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase(),
                "CUST-" + (i % 5 + 1),  // 5 different customers
                "PROD-" + (i % 3 + 1),  // 3 different products
                (i % 5) + 1,
                100.0 + (i * 10.0),
                "CREATED",
                LocalDateTime.now()
            );
            orderProducerService.sendOrderEvent(order);
        }

        Map<String, Object> response = new HashMap<>();
        response.put("count", count);
        response.put("status", "Bulk orders published to Kafka");

        return ResponseEntity.accepted().body(response);
    }
}

