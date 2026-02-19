package com.eda.producer.model;

import java.time.LocalDateTime;

public class Order {
    private String orderId;
    private String customerId;
    private String productId;
    private Integer quantity;
    private Double totalAmount;
    private String status;
    private LocalDateTime createdAt;

    public Order() {
    }

    public Order(String orderId, String customerId, String productId, Integer quantity, Double totalAmount, String status, LocalDateTime createdAt) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.productId = productId;
        this.quantity = quantity;
        this.totalAmount = totalAmount;
        this.status = status;
        this.createdAt = createdAt;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }

    public Double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(Double totalAmount) {
        this.totalAmount = totalAmount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public String toJson() {
        return String.format(
            "{\"orderId\":\"%s\",\"customerId\":\"%s\",\"productId\":\"%s\",\"quantity\":%d,\"totalAmount\":%.2f,\"status\":\"%s\",\"createdAt\":\"%s\"}",
            orderId, customerId, productId, quantity, totalAmount, status, createdAt
        );
    }
}

