package com.numaansystems.solace.commons.pipeline;

/**
 * A pass-through {@link MessageFilter} that accepts every message.
 *
 * <p>Use this as a placeholder when no filtering is required.
 *
 * @param <T> the payload type
 */
public class NoOpFilter<T> implements MessageFilter<T> {

    @Override
    public boolean accept(T payload) {
        return true;
    }
}
