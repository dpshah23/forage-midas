package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class TaskThreeTests {

    static final Logger logger = LoggerFactory.getLogger(TaskThreeTests.class);

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Test
    void task_three_verifier() throws InterruptedException {
        // 1. Populate users including Waldorf
        UserRecord waldorf = new UserRecord("Waldorf", 1000f); // starting balance
        userRepository.save(waldorf);

        userPopulator.populate(); // populate other users

        long waldorfId = waldorf.getId(); // save ID for reference

        // 2. Send all transactions
        String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }

        // 3. Wait for Kafka to process transactions and refresh user data
        Instant start = Instant.now();
        UserRecord finalWaldorf = null;
        while (Duration.between(start, Instant.now()).toSeconds() < 10) { // max 10s wait
            Optional<UserRecord> opt = userRepository.findById(waldorfId);
            if (opt.isPresent()) {
                finalWaldorf = opt.get();
                break;
            }
            Thread.sleep(200); // short delay before retry
        }

        if (finalWaldorf == null) {
            throw new RuntimeException("Waldorf not found after sending transactions");
        }

        // 4. Log final balance
        logger.info("----------------------------------------------------------");
        logger.info("Waldorf's final balance: {}", finalWaldorf.getBalance());
        logger.info("----------------------------------------------------------");
    }
}
