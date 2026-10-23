package com.concepts.controller;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/sqs")
public class SQSController {

    @Autowired
    private SqsTemplate sqsTemplate;

    @Value("${sqs.queue.standard}")
    private String standardQueue;

    @Value("${sqs.queue.fifo}")
    private String fifoQueue;

    /** Standard queue: just send the order. */
    @PostMapping("/standard")
    public String sendOrder(@RequestParam String order) {
        sqsTemplate.send(standardQueue, order);
        return "msg sent successfully";
    }

    /** FIFO queue: send the order with a group ID. */
    @PostMapping("/fifo")
    public String sendFifoOrder(@RequestParam String order, @RequestParam String groupId) {
        sqsTemplate.send(to -> to
                .queue(fifoQueue)
                .payload(order)
                .messageGroupId(groupId)
                .messageDeduplicationId(UUID.randomUUID().toString()));
        return "msg sent successfully";
    }
}