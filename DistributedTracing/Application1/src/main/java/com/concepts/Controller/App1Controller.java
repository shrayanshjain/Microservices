package com.concepts.Controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestTemplate;

@RestController
public class App1Controller {

    @Autowired
    private RestClient restClient;

    @GetMapping("/api")
    public String invokeApp2() {
        String response = restClient.get()
                .uri("http://localhost:8082/api/hello")
                .retrieve()
                .body(String.class);
        return "Response from App2: " + response;
    }
}
