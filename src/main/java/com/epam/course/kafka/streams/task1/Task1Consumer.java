package com.epam.course.kafka.streams.task1;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;

@Slf4j
@Configuration
public class Task1Consumer {

    @KafkaListener(topics = "${kafka.topics.task-1-topic-2.name}", groupId = "consumer-group-1")
    public void read(String message) {
        log.info("Read message '{}'", message);
    }
}
