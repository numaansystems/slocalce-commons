package com.numaansystems.solace.commons.consumer;

import com.numaansystems.solace.commons.config.SubscriptionDescriptor;
import com.numaansystems.solace.commons.connection.SolaceSessionWrapper;
import com.numaansystems.solace.commons.pipeline.MessageProcessingPipeline;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

/**
 * Subscribes to one Solace topic and delivers every incoming message to a
 * {@link MessageProcessingPipeline} until the consumer is explicitly closed.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>Call {@link #start()} to connect and begin receiving.</li>
 *   <li>Call {@link #close()} (or use try-with-resources) to unsubscribe and
 *       release resources.</li>
 * </ol>
 */
public class StreamingConsumer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(StreamingConsumer.class);

    private final SolaceSessionWrapper sessionWrapper;
    private final SubscriptionDescriptor descriptor;
    private final MessageProcessingPipeline<?> pipeline;

    private XMLMessageConsumer consumer;

    public StreamingConsumer(SolaceSessionWrapper sessionWrapper,
                              SubscriptionDescriptor descriptor,
                              MessageProcessingPipeline<?> pipeline) {
        this.sessionWrapper = sessionWrapper;
        this.descriptor = descriptor;
        this.pipeline = pipeline;
    }

    /**
     * Connect to the broker, add the subscription, and start receiving messages.
     *
     * @throws JCSMPException if the connection or subscription fails
     */
    public void start() throws JCSMPException {
        log.info("Starting StreamingConsumer for topic={}", descriptor.getTopic());
        sessionWrapper.connect();
        sessionWrapper.addSubscription(descriptor.getTopic());
        consumer = sessionWrapper.createConsumer(new XMLMessageListener() {
            @Override
            public void onReceive(com.solacesystems.jcsmp.BytesXMLMessage rawMessage) {
                pipeline.process(rawMessage);
            }
            @Override
            public void onException(JCSMPException e) {
                log.error("StreamingConsumer received error event on topic={}: {}",
                        descriptor.getTopic(), e.getMessage(), e);
            }
        });
        consumer.start();
        log.info("StreamingConsumer started – listening on topic={}", descriptor.getTopic());
    }

    /**
     * Stop the consumer and release all associated resources.
     */
    @Override
    public void close() {
        log.info("Stopping StreamingConsumer for topic={}", descriptor.getTopic());
        sessionWrapper.close();
    }
}
