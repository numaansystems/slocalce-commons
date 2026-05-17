package com.numaansystems.solace.commons.enricher;

import com.numaansystems.solace.commons.model.ProcessedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

/**
 * Executes a sequence of {@link MessageEnricher}s in order, passing the output
 * of each enricher as the input to the next.
 *
 * @param <T> the payload type shared across all enrichers in this chain
 */
public class EnricherChain<T> {

    private static final Logger log = LoggerFactory.getLogger(EnricherChain.class);

    private final List<MessageEnricher<T>> enrichers;

    public EnricherChain(List<MessageEnricher<T>> enrichers) {
        this.enrichers = List.copyOf(enrichers);
    }

    /**
     * Apply all enrichers in order.
     *
     * @param message the message entering the chain
     * @return the message after all enrichers have been applied
     */
    @SuppressWarnings("unchecked")
    public ProcessedMessage<T> enrich(ProcessedMessage<T> message) {
        ProcessedMessage<T> current = message;
        for (MessageEnricher<T> enricher : enrichers) {
            try {
                current = enricher.enrich(current);
                if (current == null) {
                    log.warn("Enricher {} returned null; stopping chain", enricher.getClass().getSimpleName());
                    return message; // fall back to original
                }
            } catch (Exception e) {
                log.error("Enricher {} threw an exception; skipping", enricher.getClass().getSimpleName(), e);
            }
        }
        return current;
    }

    public List<MessageEnricher<T>> getEnrichers() {
        return enrichers;
    }
}
