package com.numaansystems.solace.commons.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.messaging.MessageHeaders;
import java.util.Map;

/**
 * Default {@link MessageParser} that deserializes a JSON byte array into a
 * {@code Map<String, Object>}. Can be overridden or replaced by providing a
 * custom {@link MessageParser} bean mapped to the topic in configuration.
 */
public class JsonMessageParser implements MessageParser<Map<String, Object>> {

    private final ObjectMapper objectMapper;

    public JsonMessageParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> parse(byte[] payload, MessageHeaders headers) throws Exception {
        return objectMapper.readValue(payload, Map.class);
    }
}
