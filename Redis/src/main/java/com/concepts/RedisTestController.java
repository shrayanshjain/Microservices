package com.concepts;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RedisTestController {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @GetMapping("/redis/test")
    public String test() {
        redisTemplate.opsForValue().set("greeting", "hello world");

        return redisTemplate.opsForValue().get("greeting");
    }

}