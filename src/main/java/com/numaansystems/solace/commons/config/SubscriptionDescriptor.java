package com.numaansystems.solace.commons.config;

/**
 * Describes a single Solace topic subscription and its consumption behaviour.
 *
 * <pre>
 * solace:
 *   commons:
 *     subscriptions:
 *       - topic: market/prices/AAPL
 *         mode: STREAMING
 *       - topic: market/snapshot/AAPL
 *         mode: SNAPSHOT
 *         stop-on-first-snapshot: true
 *       - topic: market/response/AAPL
 *         mode: TRIGGER_THEN_CONSUME
 *         trigger-topic: market/request
 *         trigger-payload: '{"symbol":"AAPL"}'
 * </pre>
 */
public class SubscriptionDescriptor {

    /** Solace topic string to subscribe to. Wildcards (*) are supported. */
    private String topic;

    /** Consumer mode for this subscription. Defaults to {@link ConsumerMode#STREAMING}. */
    private ConsumerMode mode = ConsumerMode.STREAMING;

    /**
     * For {@link ConsumerMode#TRIGGER_THEN_CONSUME}: topic to publish the trigger
     * message on.
     */
    private String triggerTopic;

    /**
     * For {@link ConsumerMode#TRIGGER_THEN_CONSUME}: payload of the trigger
     * message (sent as a UTF-8 text message).
     */
    private String triggerPayload;

    /**
     * For {@link ConsumerMode#SNAPSHOT}: stop consuming after the first message
     * is received.
     */
    private boolean stopOnFirstSnapshot = false;

    // -------------------------------------------------------------------------
    // Getters and setters
    // -------------------------------------------------------------------------

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public ConsumerMode getMode() {
        return mode;
    }

    public void setMode(ConsumerMode mode) {
        this.mode = mode;
    }

    public String getTriggerTopic() {
        return triggerTopic;
    }

    public void setTriggerTopic(String triggerTopic) {
        this.triggerTopic = triggerTopic;
    }

    public String getTriggerPayload() {
        return triggerPayload;
    }

    public void setTriggerPayload(String triggerPayload) {
        this.triggerPayload = triggerPayload;
    }

    public boolean isStopOnFirstSnapshot() {
        return stopOnFirstSnapshot;
    }

    public void setStopOnFirstSnapshot(boolean stopOnFirstSnapshot) {
        this.stopOnFirstSnapshot = stopOnFirstSnapshot;
    }

    @Override
    public String toString() {
        return "SubscriptionDescriptor{topic='" + topic + "', mode=" + mode + '}';
    }
}
