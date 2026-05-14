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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Connects to a Solace topic, receives messages up to a configured timeout, and
 * optionally stops after the first message (useful for retained/snapshot topics).
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>Call {@link #start()} – this method <em>blocks</em> until the timeout
 *       expires or (when {@code stopOnFirstSnapshot=true}) the first message is
 *       delivered.</li>
 *   <li>Resources are released automatically when {@link #start()} returns, or
 *       you can call {@link #close()} explicitly.</li>
 * </ol>
 */
public class SnapshotConsumer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SnapshotConsumer.class);

    private final SolaceSessionWrapper sessionWrapper;
    private final SubscriptionDescriptor descriptor;
    private final MessageProcessingPipeline<?> pipeline;
    private final int timeoutMs;

    private final AtomicBoolean firstReceived = new AtomicBoolean(false);
    private final CountDownLatch latch = new CountDownLatch(1);

    public SnapshotConsumer(SolaceSessionWrapper sessionWrapper,
                             SubscriptionDescriptor descriptor,
                             MessageProcessingPipeline<?> pipeline,
                             int timeoutMs) {
        this.sessionWrapper = sessionWrapper;
        this.descriptor = descriptor;
        this.pipeline = pipeline;
        this.timeoutMs = timeoutMs;
    }

    /**
     * Connect, subscribe, receive snapshot messages, then disconnect.
     *
     * <p>When {@link SubscriptionDescriptor#isStopOnFirstSnapshot()} is
     * {@code true}, this method returns as soon as the first message is
     * processed.  Otherwise it waits for {@code timeoutMs} milliseconds.
     *
     * @throws JCSMPException       on connection or subscription error
     * @throws InterruptedException if the waiting thread is interrupted
     */
    public void start() throws JCSMPException, InterruptedException {
        log.info("Starting SnapshotConsumer for topic={} stopOnFirst={} timeoutMs={}",
                descriptor.getTopic(), descriptor.isStopOnFirstSnapshot(), timeoutMs);
        sessionWrapper.connect();
        sessionWrapper.addSubscription(descriptor.getTopic());

        XMLMessageConsumer consumer = sessionWrapper.createConsumer(new XMLMessageListener() {
            @Override
            public void onReceive(com.solacesystems.jcsmp.BytesXMLMessage rawMessage) {
                pipeline.process(rawMessage);
                if (descriptor.isStopOnFirstSnapshot() && firstReceived.compareAndSet(false, true)) {
                    latch.countDown();
                }
            }
            @Override
            public void onException(JCSMPException e) {
                log.error("SnapshotConsumer received error event on topic={}: {}",
                        descriptor.getTopic(), e.getMessage(), e);
                latch.countDown();
            }
        });
        consumer.start();

        boolean completed;
        if (descriptor.isStopOnFirstSnapshot()) {
            completed = latch.await(timeoutMs, TimeUnit.MILLISECONDS);
            if (!completed) {
                log.warn("SnapshotConsumer timed out after {}ms waiting for first message on {}",
                        timeoutMs, descriptor.getTopic());
            }
        } else {
            completed = latch.await(timeoutMs, TimeUnit.MILLISECONDS);
        }
        log.info("SnapshotConsumer finished for topic={} firstReceived={}",
                descriptor.getTopic(), firstReceived.get());
    }

    /**
     * Signal the consumer to stop before the timeout expires (e.g. from another
     * thread).
     */
    public void stop() {
        latch.countDown();
    }

    /** Release all resources associated with this consumer. */
    @Override
    public void close() {
        log.debug("Closing SnapshotConsumer for topic={}", descriptor.getTopic());
        sessionWrapper.close();
    }
}
