package com.numaansystems.solace.commons.model;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

/**
 * Immutable envelope that wraps a decoded Solace message payload together with
 * routing and metadata extracted from the raw JCSMP message.
 *
 * @param <T> decoded payload type
 */
public class SolaceMessage<T> {

    private final T payload;
    private final String topic;
    private final String messageId;
    private final String correlationId;
    private final Map<String, Object> userProperties;
    private final Instant receivedAt;

    public SolaceMessage(T payload,
                         String topic,
                         String messageId,
                         String correlationId,
                         Map<String, Object> userProperties) {
        this.payload = payload;
        this.topic = topic;
        this.messageId = messageId;
        this.correlationId = correlationId;
        this.userProperties = userProperties != null
                ? Collections.unmodifiableMap(userProperties)
                : Collections.emptyMap();
        this.receivedAt = Instant.now();
    }

    public T getPayload() {
        return payload;
    }

    public String getTopic() {
        return topic;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public Map<String, Object> getUserProperties() {
        return userProperties;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }

    @Override
    public String toString() {
        return "SolaceMessage{topic='" + topic + "', messageId='" + messageId
                + "', correlationId='" + correlationId + "', receivedAt=" + receivedAt + '}';
    }
}
