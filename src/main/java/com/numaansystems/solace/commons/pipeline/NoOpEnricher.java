package com.numaansystems.solace.commons.pipeline;

import com.solacesystems.jcsmp.BytesXMLMessage;

/**
 * A pass-through {@link MessageEnricher} that returns the payload unchanged.
 *
 * <p>Use this as a placeholder when no enrichment is required.
 *
 * @param <T> the payload type
 */
public class NoOpEnricher<T> implements MessageEnricher<T> {

    @Override
    public T enrich(T payload, BytesXMLMessage rawMessage) {
        return payload;
    }
}
