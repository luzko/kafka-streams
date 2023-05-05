package com.epam.course.kafka.streams.task4;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class MessageDeserializer implements Deserializer<Message> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Message deserialize(String topic, byte[] bytes) {
        try {
            if (bytes == null) {
                return null;
            }
            return objectMapper.readValue(new String(bytes, StandardCharsets.UTF_8), Message.class);
        } catch (Exception e) {
            throw new SerializationException();
        }
    }

    @Override
    public void close() {

    }
}

