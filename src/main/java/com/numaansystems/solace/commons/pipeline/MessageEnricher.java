package com.numaansystems.solace.commons.pipeline;

import com.solacesystems.jcsmp.BytesXMLMessage;

/**
 * Enriches or transforms a decoded payload before it is handed to the
 * {@link MessageHandler}.
 *
 * <p>Implementations can add metadata (e.g. tenant context, sequence numbers),
 * perform lookups, or convert the payload to a richer domain model.
 *
 * @param <T> the payload type
 */
@FunctionalInterface
public interface MessageEnricher<T> {

    /**
     * Enrich (or transform) the given payload.
     *
     * @param payload    the decoded payload
     * @param rawMessage the original Solace message (read-only context)
     * @return the enriched payload (may be the same instance or a new one)
     */
    T enrich(T payload, BytesXMLMessage rawMessage);
}
