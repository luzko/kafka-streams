package com.epam.course.kafka.streams.task3;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

@Slf4j
@Configuration
public class Task3Consumer {

    @KafkaListener(topics = "${kafka.topics.task-3-topic-1.name}", groupId = "consumer-group-1")
    public void readTopic1(String message) {
        log.info("Read message '{}'", message);
    }

    @KafkaListener(topics = "${kafka.topics.task-3-topic-2.name}", groupId = "consumer-group-1")
    public void readTopic2(String message) {
        log.info("Read message '{}'", message);
    }
}
