package com.concepts.Listener;
import io.awspring.cloud.sqs.annotation.SqsListener;
import io.awspring.cloud.sqs.listener.SqsHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class SqsListeners {

    /** Standard queue. Runs for every message. Returning normally means the message is deleted. */
    @SqsListener("${sqs.queue.standard}")
    public void onOrder(String order) {
        System.out.println("STANDARD queue: " +  order);
    }

    /** FIFO queue. The framework keeps the order inside each group. */
    @SqsListener("${sqs.queue.fifo}")
    public void onFifoOrder(String order,
                            @Header(SqsHeaders.MessageSystemAttributes.SQS_MESSAGE_GROUP_ID_HEADER) String groupId) {
        System.out.println("FIFO queue: " +  order + " group id: " + groupId);
    }
}