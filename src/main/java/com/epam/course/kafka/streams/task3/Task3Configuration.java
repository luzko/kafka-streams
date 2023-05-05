package com.epam.course.kafka.streams.task3;

import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Slf4j
public class Task3Configuration {

    private static final String JOINED_VALUES = "Joined key={}, value={}";
    private static final String LOG_MESSAGE_SPLIT_KEY_VALUE = "Split key={}, value={}";

    @Value("${kafka.topics.task-3-topic-1.name}")
    private String sourceTopic1;
    @Value("${kafka.topics.task-3-topic-2.name}")
    private String sourceTopic2;

    @Bean
    public KStream<String, String> task3Stream(KStream<String, String> source1, KStream<String, String> source2) {
        KStream<String, String> joinedStream = source1.join(
            source2, (a, b) -> a + " - " + b,
            JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(30)),
            StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()).withName("joined-stream"));
        joinedStream.peek((key, value) -> log.info(JOINED_VALUES, key, value));
        return joinedStream;
    }

    @Bean
    public KStream<String, String> source1(StreamsBuilder streamsBuilder) {
        KStream<String, String> stream = streamsBuilder
            .stream(sourceTopic1, Consumed.with(Serdes.String(), Serdes.String()));
        process(stream);
        return stream;
    }

    @Bean
    public KStream<String, String> source2(StreamsBuilder streamsBuilder) {
        KStream<String, String> stream = streamsBuilder
            .stream(sourceTopic2, Consumed.with(Serdes.String(), Serdes.String()));
        process(stream);
        return stream;
    }

    private void process(KStream<String, String> stream) {
        stream.filter((key, value) -> value != null && value.contains(":"))
            .map((key, value) -> split(value))
            .peek((key, value) -> log.info(LOG_MESSAGE_SPLIT_KEY_VALUE, key, value));
    }

    private KeyValue<String, String> split(String value) {
        String[] pair = value.split(":");
        return KeyValue.pair(pair[0], pair[1]);
    }

    @Bean
    NewTopic task3topic1(@Value("${kafka.topics.task-3-topic-1.name}") String topic,
                         @Value("${kafka.topics.task-3-topic-1.partitions}") int partitions,
                         @Value("${kafka.topics.task-3-topic-1.replicas}") int replicas) {
        return generateKafkaTopic(topic, partitions, replicas);
    }

    @Bean
    NewTopic task3topic2(@Value("${kafka.topics.task-3-topic-2.name}") String topic,
                         @Value("${kafka.topics.task-3-topic-2.partitions}") int partitions,
                         @Value("${kafka.topics.task-3-topic-2.replicas}") int replicas) {
        return generateKafkaTopic(topic, partitions, replicas);
    }

    private NewTopic generateKafkaTopic(String topicName, int partitions, int replicas) {
        return TopicBuilder.name(topicName)
            .partitions(partitions)
            .replicas(replicas)
            .build();
    }
}
