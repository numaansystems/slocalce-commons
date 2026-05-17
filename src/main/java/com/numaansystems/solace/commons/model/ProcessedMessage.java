package com.numaansystems.solace.commons.model;

import org.springframework.messaging.MessageHeaders;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Carrier object for a Solace message as it flows through the processing pipeline.
 * Holds the parsed payload, original message headers, source topic, and a mutable
 * metadata map that enrichers can use to annotate the message.
 *
 * @param <T> the parsed payload type
 */
public final class ProcessedMessage<T> {

    private final T payload;
    private final MessageHeaders headers;
    private final String sourceTopic;
    private final Map<String, Object> metadata;

    private ProcessedMessage(T payload, MessageHeaders headers, String sourceTopic,
                             Map<String, Object> metadata) {
        this.payload = payload;
        this.headers = headers;
        this.sourceTopic = sourceTopic;
        this.metadata = Collections.unmodifiableMap(new HashMap<>(metadata));
    }

    public static <T> ProcessedMessage<T> of(T payload, MessageHeaders headers, String sourceTopic) {
        return new ProcessedMessage<>(payload, headers, sourceTopic, new HashMap<>());
    }

    public T getPayload() { return payload; }

    public MessageHeaders getHeaders() { return headers; }

    public String getSourceTopic() { return sourceTopic; }

    public Map<String, Object> getMetadata() { return metadata; }

    /**
     * Returns a new ProcessedMessage with the given metadata entry added or updated.
     */
    public ProcessedMessage<T> withMetadata(String key, Object value) {
        Map<String, Object> newMeta = new HashMap<>(this.metadata);
        newMeta.put(key, value);
        return new ProcessedMessage<>(this.payload, this.headers, this.sourceTopic, newMeta);
    }

    /**
     * Returns a new ProcessedMessage with a different payload (e.g., after enrichment).
     */
    public <R> ProcessedMessage<R> withPayload(R newPayload) {
        return new ProcessedMessage<>(newPayload, this.headers, this.sourceTopic, this.metadata);
    }

    @Override
    public String toString() {
        return "ProcessedMessage{topic='" + sourceTopic + "', payload=" + payload + '}';
    }
}
