package com.numaansystems.solace.commons.connection;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

/**
 * Thin, lifecycle-aware wrapper around a {@link JCSMPSession}.
 *
 * <p>Simplifies the raw JCSMP API and makes the connection layer easier to
 * stub/mock in unit tests.
 */
public class SolaceSessionWrapper implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SolaceSessionWrapper.class);

    private final JCSMPSession session;
    private XMLMessageConsumer consumer;
    private XMLMessageProducer producer;

    public SolaceSessionWrapper(JCSMPSession session) {
        this.session = session;
    }

    /**
     * Establish the TCP connection to the Solace broker.
     *
     * @throws JCSMPException on connection failure
     */
    public void connect() throws JCSMPException {
        log.debug("Connecting Solace session …");
        session.connect();
        log.info("Solace session connected");
    }

    /**
     * Add a topic subscription to the session.
     *
     * @param topicName the Solace topic string (wildcards supported)
     * @throws JCSMPException if the subscription cannot be added
     */
    public void addSubscription(String topicName) throws JCSMPException {
        log.debug("Adding subscription: {}", topicName);
        session.addSubscription(JCSMPFactory.onlyInstance().createTopic(topicName));
    }

    /**
     * Remove a topic subscription from the session.
     *
     * @param topicName the topic to unsubscribe from
     * @throws JCSMPException if the subscription cannot be removed
     */
    public void removeSubscription(String topicName) throws JCSMPException {
        log.debug("Removing subscription: {}", topicName);
        session.removeSubscription(JCSMPFactory.onlyInstance().createTopic(topicName));
    }

    /**
     * Create (or return the existing) message consumer for this session.
     *
     * @param listener message listener callback
     * @return the consumer (not yet started)
     * @throws JCSMPException on error
     */
    public XMLMessageConsumer createConsumer(XMLMessageListener listener) throws JCSMPException {
        consumer = session.getMessageConsumer(listener);
        return consumer;
    }

    /**
     * Create (or return the existing) message producer for this session.
     *
     * @param eventHandler publish event handler (can be {@code null})
     * @return the producer
     * @throws JCSMPException on error
     */
    public XMLMessageProducer createProducer(
            JCSMPStreamingPublishCorrelatingEventHandler eventHandler) throws JCSMPException {
        producer = session.getMessageProducer(eventHandler);
        return producer;
    }

    /** Returns the underlying JCSMP session (for advanced use-cases). */
    public JCSMPSession getSession() {
        return session;
    }

    /**
     * Close the consumer, producer, and session, releasing all resources.
     */
    @Override
    public void close() {
        if (consumer != null) {
            try {
                consumer.close();
            } catch (Exception e) {
                log.warn("Error closing consumer", e);
            }
        }
        if (producer != null) {
            try {
                producer.close();
            } catch (Exception e) {
                log.warn("Error closing producer", e);
            }
        }
        try {
            session.closeSession();
            log.info("Solace session closed");
        } catch (Exception e) {
            log.warn("Error closing session", e);
        }
    }
}
