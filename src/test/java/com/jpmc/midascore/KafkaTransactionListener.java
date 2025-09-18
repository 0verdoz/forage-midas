package com.jpmc.midascore;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.foundation.Transaction;

@Component
public class KafkaTransactionListener {

    @Value("${general.kafka-topic}")
    private String topic;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group", containerFactory = "kafkaListenerContainerFactory")
    public void listen(String message) {
        try {
            Transaction transaction = objectMapper.readValue(message, Transaction.class);
            System.out.println("Received transaction: " + transaction);
        } catch (Exception e) {
            System.err.println("Failed to parse transaction message: " + e.getMessage());
        }
    }
}
