package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionConsumer {
    private final TransactionProcessor transactionProcessor;

    public TransactionConsumer(TransactionProcessor transactionProcessor) {
        this.transactionProcessor = transactionProcessor;
    }

    @KafkaListener(topics = "transactions", groupId = "midas")
    public void listen(Transaction t) {
        transactionProcessor.handleTransaction(t);
    }
}
