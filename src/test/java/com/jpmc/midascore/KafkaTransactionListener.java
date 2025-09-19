package com.jpmc.midascore;

import com.jpmc.midascore.config.RestTemplateConfig;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.web.client.RestTemplate;
import com.jpmc.midascore.foundation.Incentive;

import java.util.Optional;

@Component
public class KafkaTransactionListener {

    @Value("${general.kafka-topic}")
    private String topic;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final UserRepository userRepo;
    private final TransactionRecordRepository transactionRepo;
    private final RestTemplate restTemplate;

    public KafkaTransactionListener(UserRepository userRepo, TransactionRecordRepository transactionRepo, RestTemplate restTemplate) {
        this.userRepo = userRepo;
        this.transactionRepo = transactionRepo;
        this.restTemplate = restTemplate;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group", containerFactory = "kafkaListenerContainerFactory")
    public void listen(Transaction transaction) {
        try {
//            Transaction transaction = objectMapper.readValue(message, Transaction.class);

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


            // Call the Incentives API
            String incentiveUrl = "http://localhost:8080/incentive";
            Incentive incentive = restTemplate.postForObject(incentiveUrl, transaction, Incentive.class);
            float incentiveAmount = incentive != null ? incentive.getAmount() : 0;

            if (recipient.getName().equalsIgnoreCase("wilbur") || sender.getName().equalsIgnoreCase("wilbur")) {
                System.out.println("Wilbur involved in transaction: " + transaction);
                System.out.println("Incentive: " + incentiveAmount);
                System.out.println("Before balance (recipient): " + recipient.getBalance());
            }

            // Update balances
            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

            // Save updated users
            userRepo.save(sender);
            userRepo.save(recipient);

            // Save transaction
            TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
            transactionRepo.save(record);

            System.out.println("Transaction processed and saved.");

        } catch (Exception e) {
            System.err.println("Failed to parse or process transaction: " + e.getMessage());
        }
    }
}
