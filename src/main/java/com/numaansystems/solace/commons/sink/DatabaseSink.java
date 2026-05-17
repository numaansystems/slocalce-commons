package com.numaansystems.solace.commons.sink;

import com.numaansystems.solace.commons.model.ProcessedMessage;

/**
 * {@link MessageSink} that delegates to a user-supplied {@link DatabasePersistenceHook}.
 *
 * @param <T> the payload type
 */
public class DatabaseSink<T> implements MessageSink<T> {

    private final DatabasePersistenceHook<T> hook;

    public DatabaseSink(DatabasePersistenceHook<T> hook) {
        this.hook = hook;
    }

    @Override
    public void send(ProcessedMessage<T> message) {
        hook.save(message);
    }
}
