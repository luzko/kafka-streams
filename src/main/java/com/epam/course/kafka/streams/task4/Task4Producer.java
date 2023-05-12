package com.epam.course.kafka.streams.task4;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

@Slf4j
@Configuration
public class Task4Producer {

    public void send(KafkaTemplate<String, String> kafkaTemplate, String topic, String message) {
        log.info("Send message `{}`", message);
        kafkaTemplate.send(topic, message);
    }
}
