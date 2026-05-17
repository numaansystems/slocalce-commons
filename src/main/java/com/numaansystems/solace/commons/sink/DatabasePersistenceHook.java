package com.numaansystems.solace.commons.sink;

import com.numaansystems.solace.commons.model.ProcessedMessage;

/**
 * Extension point for persisting a processed message to a data store.
 * <p>
 * Provide an implementation as a Spring bean and enable
 * {@code solace.commons.sinks.database.enabled=true} in configuration.
 * The {@link DatabaseSink} adapter delegates to this hook.
 *
 * @param <T> the payload type to persist
 */
public interface DatabasePersistenceHook<T> {

    /**
     * Persist the given processed message.
     *
     * @param message the message to save
     */
    void save(ProcessedMessage<T> message);
}
