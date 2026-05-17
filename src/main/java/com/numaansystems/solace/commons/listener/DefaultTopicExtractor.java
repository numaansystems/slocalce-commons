package com.numaansystems.solace.commons.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.MessageHeaders;
import java.util.List;

/**
 * Default {@link TopicExtractor} that checks well-known header names in order:
 * <ol>
 *   <li>{@code solace_destinationName} — set by the Solace Spring Cloud Stream binder</li>
 *   <li>{@code solace_destination} — legacy Solace header (as String)</li>
 *   <li>{@code destination} — generic Spring Cloud Stream header</li>
 * </ol>
 */
public class DefaultTopicExtractor implements TopicExtractor {

    private static final Logger log = LoggerFactory.getLogger(DefaultTopicExtractor.class);

    /** Ordered list of header keys to probe for the destination/topic name. */
    private static final List<String> CANDIDATE_HEADERS = List.of(
            "solace_destinationName",
            "solace_destination",
            "destination"
    );

    @Override
    public String extract(MessageHeaders headers) {
        for (String key : CANDIDATE_HEADERS) {
            Object value = headers.get(key);
            if (value instanceof String s && !s.isBlank()) {
                return s;
            }
            if (value != null) {
                String str = value.toString();
                if (!str.isBlank()) {
                    return str;
                }
            }
        }
        log.debug("Could not determine source topic from headers: {}", headers.keySet());
        return null;
    }
}
