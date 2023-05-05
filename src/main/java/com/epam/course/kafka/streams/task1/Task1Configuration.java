package com.epam.course.kafka.streams.task1;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class Task1Configuration {

    @Value("${kafka.topics.task-1-topic-1.name}")
    private String source;

    @Value("${kafka.topics.task-1-topic-2.name}")
    private String destination;

    @Bean
    public KStream<String, String> task1Stream(StreamsBuilder streamsBuilder) {
        KStream<String, String> stream = streamsBuilder
            .stream(source, Consumed.with(Serdes.String(), Serdes.String()));

        stream.to(destination);
        return stream;
    }

    @Bean
    NewTopic take1topic1(@Value("${kafka.topics.task-1-topic-1.name}") String topic,
                         @Value("${kafka.topics.task-1-topic-1.partitions}") int partitions,
                         @Value("${kafka.topics.task-1-topic-1.replicas}") int replicas) {
        return generateKafkaTopic(topic, partitions, replicas);
    }

    @Bean
    NewTopic take1topic2(@Value("${kafka.topics.task-1-topic-2.name}") String topic,
                         @Value("${kafka.topics.task-1-topic-2.partitions}") int partitions,
                         @Value("${kafka.topics.task-1-topic-2.replicas}") int replicas) {
        return generateKafkaTopic(topic, partitions, replicas);
    }

    private NewTopic generateKafkaTopic(String topicName, int partitions, int replicas) {
        return TopicBuilder.name(topicName)
            .partitions(partitions)
            .replicas(replicas)
            .build();
    }
}
