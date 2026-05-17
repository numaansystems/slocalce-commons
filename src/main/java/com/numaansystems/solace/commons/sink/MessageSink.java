package com.numaansystems.solace.commons.sink;

import com.numaansystems.solace.commons.model.ProcessedMessage;

/**
 * Pluggable sink for dispatching a fully processed (parsed + enriched) message.
 * <p>
 * Built-in implementations: {@link DownstreamPublishSink} and
 * {@link DatabaseSink}. Custom sinks can be registered as Spring beans and
 * referenced in the topic sink configuration.
 *
 * @param <T> the payload type accepted by this sink
 */
public interface MessageSink<T> {

    /**
     * Dispatch the processed message.
     *
     * @param message the fully enriched message
     */
    void send(ProcessedMessage<T> message);
}
