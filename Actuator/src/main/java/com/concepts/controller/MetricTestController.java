package com.concepts.controller;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MetricTestController {

    private final MeterRegistry registry;

    public MetricTestController(MeterRegistry registry) {
        this.registry = registry;
    }

    @GetMapping("/hello")
    public String hello() {
        registry.counter("myapp.hello.requests").increment();
        return "Hello, Datadog!";
    }
}