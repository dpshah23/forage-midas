package com.jpmc.midascore;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = { "transactions" })
@DirtiesContext
class TaskTwoTests {

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Test
    void task_two_verifier() throws Exception {

        // Create consumer
        Map<String, Object> consumerProps =
                KafkaTestUtils.consumerProps("testGroup", "true", embeddedKafka);

        DefaultKafkaConsumerFactory<String, String> cf =
                new DefaultKafkaConsumerFactory<>(consumerProps);

        Consumer<String, String> consumer = cf.createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "transactions");

        String[] lines = fileLoader.loadStrings("/test_data/poiuytrewq.uiop");

        for (String line : lines) {
            kafkaProducer.send(line);
        }

        // Assert messages received
        List<String> received = new ArrayList<>();
        for (int i = 0; i < lines.length; i++) {
            ConsumerRecord<String, String> record =
                    KafkaTestUtils.getSingleRecord(consumer, "transactions");
            received.add(record.value());
        }

        assertThat(received).containsExactly(lines);
    }
}
