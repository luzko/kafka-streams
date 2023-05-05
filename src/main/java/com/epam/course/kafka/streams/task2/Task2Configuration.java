package com.epam.course.kafka.streams.task2;

import java.util.Map;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Slf4j
@Configuration
public class Task2Configuration {

    private static final String WORD_SEPARATOR_REGEX = "\\W+";
    private static final String SENTENCE_INTO_WORDS = "Sentence into words: key={}, value={}";
    private static final String WORDS_THAT_CONTAIN_LETTER = "Words that contain letter 'a': key={}, value={}";
    private static final String COMMON_BRANCH = "words-";
    private static final String SHORT = "short";
    private static final String LONG = "long";
    public static final String TARGET_CHARACTER = "a";

    @Value(value = "${spring.kafka.topics.task2-source}")
    private String topic;

    @Bean
    public KStream<Long, String> task2Stream(StreamsBuilder streamsBuilder) {
        KStream<Long, String> stream = streamsBuilder.stream(topic, Consumed.with(Serdes.Long(), Serdes.String()));

        Map<String, KStream<Integer, String>> branchedWordsStream = stream.filter((key, value) -> value != null)
            .flatMap((key, value) -> Stream.of(value.split(WORD_SEPARATOR_REGEX))
                .map(word -> KeyValue.pair(word.length(), word))
                .toList())
            .peek((key, value) -> log.info(SENTENCE_INTO_WORDS, key, value))
            .split(Named.as(COMMON_BRANCH))
            .branch((key, value) -> value.length() < 10, Branched.as(SHORT))
            .defaultBranch(Branched.as(LONG));

        KStream<Integer, String> filteredShortWordStream =
            filterWords(branchedWordsStream.get(COMMON_BRANCH + SHORT));
        KStream<Integer, String> filteredLongWordStream =
            filterWords(branchedWordsStream.get(COMMON_BRANCH + LONG));

        filteredShortWordStream.merge(filteredLongWordStream)
            .peek((key, value) -> log.info(WORDS_THAT_CONTAIN_LETTER, key, value));

        return stream;
    }

    private KStream<Integer, String> filterWords(KStream<Integer, String> stream) {
        return stream.filter((key, value) -> value.contains(TARGET_CHARACTER));
    }

    @Bean
    NewTopic task2topic1(@Value("${kafka.topics.task-2-topic-2.name}") String topic,
                         @Value("${kafka.topics.task-2-topic-2.partitions}") int partitions,
                         @Value("${kafka.topics.task-2-topic-2.replicas}") int replicas) {
        return TopicBuilder.name(topic)
            .partitions(partitions)
            .replicas(replicas)
            .build();
    }
}
