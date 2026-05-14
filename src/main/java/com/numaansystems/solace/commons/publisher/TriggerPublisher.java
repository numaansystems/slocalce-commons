package com.numaansystems.solace.commons.publisher;

import com.numaansystems.solace.commons.connection.SolaceSessionWrapper;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes trigger messages to a Solace topic as part of the
 * trigger-then-consume pattern.
 *
 * <p>Each published message can optionally carry an {@code applicationMessageId}
 * (used as a correlation handle) so that downstream response messages can be
 * matched back to this request.
 */
public class TriggerPublisher {

    private static final Logger log = LoggerFactory.getLogger(TriggerPublisher.class);

    private final SolaceSessionWrapper sessionWrapper;

    public TriggerPublisher(SolaceSessionWrapper sessionWrapper) {
        this.sessionWrapper = sessionWrapper;
    }

    /**
     * Publish a text trigger message to the given topic.
     *
     * @param topicName     the Solace topic to publish to
     * @param payload       text payload (may be {@code null} for an empty message)
     * @param correlationId correlation ID embedded in the message (may be {@code null})
     * @throws JCSMPException if the message cannot be sent
     */
    public void publish(String topicName, String payload, String correlationId)
            throws JCSMPException {

        log.debug("Publishing trigger to topic={} correlationId={}", topicName, correlationId);

        XMLMessageProducer producer = sessionWrapper.createProducer(null);

        TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
        if (payload != null) {
            message.setText(payload);
        }
        if (correlationId != null) {
            message.setCorrelationId(correlationId);
            message.setApplicationMessageId(correlationId);
        }

        Topic topic = JCSMPFactory.onlyInstance().createTopic(topicName);
        producer.send(message, topic);
        log.info("Trigger published to topic={} correlationId={}", topicName, correlationId);
    }
}
