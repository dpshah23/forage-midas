package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.repository.TransactionRepository;
import org.springframework.stereotype.Component;

@Component
public class TransactionProcessor {

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;

    public TransactionProcessor(UserRepository userRepository,
                                TransactionRepository transactionRepository) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
    }

    public void handleTransaction(Transaction t) {

        var senderOpt = userRepository.findById(t.getSenderId());
        var recipientOpt = userRepository.findById(t.getRecipientId());

        if (senderOpt.isEmpty() || recipientOpt.isEmpty()) {
            return; // invalid
        }

        UserRecord sender = senderOpt.get();
        UserRecord recipient = recipientOpt.get();

        if (sender.getBalance() < t.getAmount()) {
            return; // insufficient funds
        }

        sender.setBalance(sender.getBalance() - t.getAmount());
        recipient.setBalance(recipient.getBalance() + t.getAmount());

        userRepository.save(sender);
        userRepository.save(recipient);

        TransactionRecord record =
                new TransactionRecord(sender, recipient, t.getAmount());

        transactionRepository.save(record);
    }
}
