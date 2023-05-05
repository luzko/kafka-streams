package com.epam.course.kafka.streams.task4;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Slf4j
@Configuration
public class Task4Configuration {

    @Value("${kafka.topics.task-4-topic-1.name}")
    private String topic;

    @Bean
    public void task4Stream(StreamsBuilder streamsBuilder) {
        Serde<Message> serde = Serdes.serdeFrom(new MessageSerializer(), new MessageDeserializer());
        KStream<String, Message> stream = streamsBuilder
            .stream(topic, Consumed.with(Serdes.String(), serde));

        stream.filter((key, value) -> value != null)
            .peek((key, value) -> log.info("key={}, value={}", key, value));
    }

    @Bean
    NewTopic task4Topic4(@Value("${kafka.topics.task-4-topic-1.name}") String topic,
                         @Value("${kafka.topics.task-4-topic-1.partitions}") int partitions,
                         @Value("${kafka.topics.task-4-topic-1.replicas}") int replicas) {
        return TopicBuilder.name(topic)
            .partitions(partitions)
            .replicas(replicas)
            .build();
    }
}
