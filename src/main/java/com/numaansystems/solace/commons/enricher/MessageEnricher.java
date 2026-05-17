package com.numaansystems.solace.commons.enricher;

import com.numaansystems.solace.commons.model.ProcessedMessage;

/**
 * Extension point for enriching a {@link ProcessedMessage} during pipeline processing.
 * <p>
 * Implementations should be Spring beans. They are invoked in the order declared in
 * the topic configuration under {@code solace.commons.topics.<topic>.enrichers}.
 *
 * @param <T> the payload type this enricher operates on
 */
public interface MessageEnricher<T> {

    /**
     * Enrich the given message and return (a possibly new) {@link ProcessedMessage}.
     * Enrichers may add metadata, transform the payload, or call external services.
     *
     * @param message the current pipeline message
     * @return the enriched message; never {@code null}
     */
    ProcessedMessage<T> enrich(ProcessedMessage<T> message);
}
