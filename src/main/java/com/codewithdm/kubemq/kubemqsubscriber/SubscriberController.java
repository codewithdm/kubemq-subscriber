package com.codewithdm.kubemq.kubemqsubscriber;

import io.kubemq.sdk.basic.ServerAddressNotSuppliedException;
import io.kubemq.sdk.queue.Message;
import io.kubemq.sdk.queue.Queue;
import io.kubemq.sdk.queue.ReceiveMessagesResponse;
import io.kubemq.sdk.tools.Converter;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@RestController
@RequestMapping("/subscriber")
public class SubscriberController {

    @GetMapping("/receive")
    public List<String> receiveMessgae() throws ServerAddressNotSuppliedException, IOException, ClassNotFoundException {
        List<String> messages = new ArrayList<>();
        Queue queue = new Queue("my-queue", "hello-world-sender", "localhost:50000");
        ReceiveMessagesResponse resRec = queue.ReceiveQueueMessages(10, 1);
        if (resRec.getIsError()) {
            System.out.printf("Message dequeue error, error: %s", resRec.getError());
        }
        System.out.printf("Received Messages %s:", resRec.getMessagesReceived());
        for (Message msg : resRec.getMessages()) {
            System.out.printf("MessageID: %s, Body:%s", msg.getMessageID(), Converter.FromByteArray(msg.getBody()));
            messages.add((String) Converter.FromByteArray(msg.getBody()));
        }
        return messages;
    }
}
