package com.numaansystems.solace.commons.config;

/**
 * Defines how a given topic subscription should be consumed.
 */
public enum ConsumerMode {

    /**
     * Continuously receive messages as they arrive on the subscribed topic.
     */
    STREAMING,

    /**
     * Receive messages up to a configurable timeout and, optionally, stop after
     * the first message arrives (useful for retained/snapshot payloads).
     */
    SNAPSHOT,

    /**
     * Publish a trigger message to a configured trigger topic, then consume
     * correlated response messages from the subscription topic.
     */
    TRIGGER_THEN_CONSUME
}
