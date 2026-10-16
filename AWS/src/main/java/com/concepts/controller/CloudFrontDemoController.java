package com.concepts.controller;

import com.concepts.service.CloudFrontService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/cdn")
public class CloudFrontDemoController {

    @Autowired
    private CloudFrontService urls;

    @GetMapping("/signed-url")
    public String signedUrl(@RequestParam String key) {
        return urls.signedUrl(key);
    }
}
