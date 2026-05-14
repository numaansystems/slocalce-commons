package com.numaansystems.solace.commons.pipeline;

import com.numaansystems.solace.commons.model.SolaceMessage;

/**
 * Terminal step in the processing pipeline – handles the fully decoded and
 * enriched payload together with its message envelope.
 *
 * <p>Implement this in your microservice to perform business logic (persist,
 * forward, aggregate, …).
 *
 * @param <T> the payload type
 */
@FunctionalInterface
public interface MessageHandler<T> {

    /**
     * Handle the processed payload.
     *
     * @param payload the decoded and enriched payload
     * @param message the full message envelope (topic, IDs, user-properties …)
     */
    void handle(T payload, SolaceMessage<T> message);
}
