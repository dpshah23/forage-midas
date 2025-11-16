package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionListner {

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core")
    public void listen(String message) {
        try {
            // message looks like: "6, 7, 122.86"
            String[] parts = message.split(",");

            long senderId = Long.parseLong(parts[0].trim());
            long recipientId = Long.parseLong(parts[1].trim());
            float amount = Float.parseFloat(parts[2].trim());

            Transaction tx = new Transaction(senderId, recipientId, amount);

            System.out.println("Received: " + tx);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
