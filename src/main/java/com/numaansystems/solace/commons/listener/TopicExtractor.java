package com.numaansystems.solace.commons.listener;

import org.springframework.messaging.MessageHeaders;

/**
 * Strategy for extracting the Solace destination (topic) from message headers.
 * The default implementation, {@link DefaultTopicExtractor}, checks well-known
 * Solace binder header keys.
 */
public interface TopicExtractor {

    /**
     * Extract the topic/destination name from the given headers.
     *
     * @param headers the message headers
     * @return the topic name, or {@code null} if it cannot be determined
     */
    String extract(MessageHeaders headers);
}
