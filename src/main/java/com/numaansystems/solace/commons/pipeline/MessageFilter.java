package com.numaansystems.solace.commons.pipeline;

/**
 * Decides whether a decoded payload should continue through the pipeline.
 *
 * <p>Return {@code true} to accept the message; {@code false} to discard it.
 *
 * @param <T> the payload type
 */
@FunctionalInterface
public interface MessageFilter<T> {

    /**
     * @param payload the decoded payload
     * @return {@code true} if the message should be processed further
     */
    boolean accept(T payload);
}
