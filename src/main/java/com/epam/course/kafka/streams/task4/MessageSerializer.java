package com.epam.course.kafka.streams.task4;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class MessageSerializer implements Serializer<Message> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Message message) {
        try {
            if (message == null) {
                return null;
            }
            return objectMapper.writeValueAsBytes(message);
        } catch (Exception e) {
            throw new SerializationException();
        }
    }

    @Override
    public void close() {

    }
}
