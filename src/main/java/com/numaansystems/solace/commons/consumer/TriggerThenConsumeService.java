package com.numaansystems.solace.commons.consumer;

import com.numaansystems.solace.commons.config.SubscriptionDescriptor;
import com.numaansystems.solace.commons.connection.SolaceClientFactory;
import com.numaansystems.solace.commons.connection.SolaceSessionWrapper;
import com.numaansystems.solace.commons.pipeline.MessageProcessingPipeline;
import com.numaansystems.solace.commons.publisher.TriggerPublisher;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Implements the <em>trigger-then-consume</em> pattern:
 * <ol>
 *   <li>Subscribe to the configured response topic.</li>
 *   <li>Publish a trigger message (with a generated correlation ID) to the
 *       configured trigger topic.</li>
 *   <li>Route all incoming messages whose {@code correlationId} matches to the
 *       processing pipeline.</li>
 *   <li>Stop after the first correlated message is received, or after the
 *       configured timeout.</li>
 * </ol>
 *
 * <p>Messages that do not carry a {@code correlationId} are also forwarded to
 * the pipeline (the broker may not echo the ID back).
 */
public class TriggerThenConsumeService implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(TriggerThenConsumeService.class);

    private final SolaceClientFactory clientFactory;
    private final SubscriptionDescriptor descriptor;
    private final MessageProcessingPipeline<?> pipeline;
    private final int timeoutMs;

    private SolaceSessionWrapper sessionWrapper;

    public TriggerThenConsumeService(SolaceClientFactory clientFactory,
                                     SubscriptionDescriptor descriptor,
                                     MessageProcessingPipeline<?> pipeline,
                                     int timeoutMs) {
        this.clientFactory = clientFactory;
        this.descriptor = descriptor;
        this.pipeline = pipeline;
        this.timeoutMs = timeoutMs;
    }

    /**
     * Execute the trigger-then-consume flow.
     *
     * <p>This method blocks until a correlated response is received or the
     * timeout elapses.
     *
     * @throws JCSMPException       on connection/publish errors
     * @throws InterruptedException if the waiting thread is interrupted
     */
    public void start() throws JCSMPException, InterruptedException {
        sessionWrapper = clientFactory.createSession();
        sessionWrapper.connect();

        String correlationId = UUID.randomUUID().toString();
        log.info("TriggerThenConsume: subscribing topic={} triggerTopic={} correlationId={}",
                descriptor.getTopic(), descriptor.getTriggerTopic(), correlationId);

        sessionWrapper.addSubscription(descriptor.getTopic());

        CountDownLatch latch = new CountDownLatch(1);

        XMLMessageConsumer consumer = sessionWrapper.createConsumer(new XMLMessageListener() {
            @Override
            public void onReceive(BytesXMLMessage rawMessage) {
                String msgCorrelationId = rawMessage.getCorrelationId();
                if (msgCorrelationId == null || correlationId.equals(msgCorrelationId)) {
                    pipeline.process(rawMessage);
                    latch.countDown();
                } else {
                    log.trace("Discarding message with non-matching correlationId={}", msgCorrelationId);
                }
            }
            @Override
            public void onException(JCSMPException e) {
                log.error("TriggerThenConsumeService received error event on topic={}: {}",
                        descriptor.getTopic(), e.getMessage(), e);
                latch.countDown();
            }
        });
        consumer.start();

        // Publish trigger after consumer is ready to avoid a race condition.
        TriggerPublisher publisher = new TriggerPublisher(sessionWrapper);
        publisher.publish(descriptor.getTriggerTopic(), descriptor.getTriggerPayload(), correlationId);

        boolean received = latch.await(timeoutMs, TimeUnit.MILLISECONDS);
        if (!received) {
            log.warn("TriggerThenConsume: timed out after {}ms waiting for correlated response "
                    + "on topic={}", timeoutMs, descriptor.getTopic());
        }
    }

    /** Release all resources. */
    @Override
    public void close() {
        if (sessionWrapper != null) {
            sessionWrapper.close();
        }
    }
}
