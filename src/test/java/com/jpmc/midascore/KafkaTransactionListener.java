package com.jpmc.midascore;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.foundation.Transaction;

import java.util.Optional;

@Component
public class KafkaTransactionListener {

    @Value("${general.kafka-topic}")
    private String topic;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final UserRepository userRepo;
    private final TransactionRecordRepository transactionRepo;

    public KafkaTransactionListener(UserRepository userRepo, TransactionRecordRepository transactionRepo) {
        this.userRepo = userRepo;
        this.transactionRepo = transactionRepo;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group", containerFactory = "kafkaListenerContainerFactory")
    public void listen(String message) {
        try {
            Transaction transaction = objectMapper.readValue(message, Transaction.class);

            Optional<UserRecord> senderOpt = userRepo.findById(transaction.getSenderId());
            Optional<UserRecord> recipientOpt = userRepo.findById(transaction.getRecipientId());

            if (senderOpt.isEmpty() || recipientOpt.isEmpty()) {
                System.out.println("Invalid sender or recipient.");
                return;
            }

            UserRecord sender = senderOpt.get();
            UserRecord recipient = recipientOpt.get();

            if (sender.getBalance() < transaction.getAmount()) {
                System.out.println("Insufficient balance.");
                return;
            }

            // Update balances
            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(recipient.getBalance() + transaction.getAmount());

            // Save updated users
            userRepo.save(sender);
            userRepo.save(recipient);

            // Save transaction
            TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount());
            transactionRepo.save(record);

            System.out.println("Transaction processed and saved.");

        } catch (Exception e) {
            System.err.println("Failed to parse or process transaction: " + e.getMessage());
        }
    }
}
