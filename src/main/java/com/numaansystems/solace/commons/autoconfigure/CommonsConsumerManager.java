package com.numaansystems.solace.commons.autoconfigure;

import com.numaansystems.solace.commons.config.SolaceCommonsProperties;
import com.numaansystems.solace.commons.config.SubscriptionDescriptor;
import com.numaansystems.solace.commons.connection.SolaceClientFactory;
import com.numaansystems.solace.commons.connection.SolaceSessionWrapper;
import com.numaansystems.solace.commons.consumer.SnapshotConsumer;
import com.numaansystems.solace.commons.consumer.StreamingConsumer;
import com.numaansystems.solace.commons.consumer.TriggerThenConsumeService;
import com.numaansystems.solace.commons.pipeline.MessageProcessingPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

/**
 * Central entry-point bean that starts and stops all Solace consumers
 * configured via {@link SolaceCommonsProperties}.
 *
 * <p>It participates in the Spring application lifecycle via
 * {@link SmartLifecycle}: consumers are started after the application context
 * is fully refreshed and stopped gracefully on shutdown.
 *
 * <p>Client applications only need to:
 * <ol>
 *   <li>Add the library as a dependency and provide the required
 *       {@code solace.commons.*} properties.</li>
 *   <li>Optionally register topic-specific pipelines in a
 *       {@link PipelineRegistry} bean.</li>
 * </ol>
 */
public class CommonsConsumerManager implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(CommonsConsumerManager.class);

    private final SolaceCommonsProperties properties;
    private final SolaceClientFactory clientFactory;
    private final PipelineRegistry pipelineRegistry;

    private final List<Closeable> activeConsumers = new ArrayList<>();
    private volatile boolean running = false;

    public CommonsConsumerManager(SolaceCommonsProperties properties,
                                   SolaceClientFactory clientFactory,
                                   PipelineRegistry pipelineRegistry) {
        this.properties = properties;
        this.clientFactory = clientFactory;
        this.pipelineRegistry = pipelineRegistry;
    }

    // -------------------------------------------------------------------------
    // SmartLifecycle
    // -------------------------------------------------------------------------

    @Override
    public void start() {
        if (!properties.isEnabled()) {
            log.info("Solace Commons is disabled (solace.commons.enabled=false). "
                    + "No consumers will be started.");
            return;
        }

        log.info("CommonsConsumerManager starting – {} subscription(s) configured",
                properties.getSubscriptions().size());

        for (SubscriptionDescriptor descriptor : properties.getSubscriptions()) {
            try {
                startConsumer(descriptor);
            } catch (Exception e) {
                log.error("Failed to start consumer for topic '{}': {}",
                        descriptor.getTopic(), e.getMessage(), e);
            }
        }
        running = true;
        log.info("CommonsConsumerManager started – {} consumer(s) active", activeConsumers.size());
    }

    @Override
    public void stop() {
        log.info("CommonsConsumerManager stopping – closing {} consumer(s)", activeConsumers.size());
        for (Closeable consumer : activeConsumers) {
            try {
                consumer.close();
            } catch (Exception e) {
                log.warn("Error while closing consumer: {}", e.getMessage(), e);
            }
        }
        activeConsumers.clear();
        running = false;
        log.info("CommonsConsumerManager stopped");
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Higher phase number means this bean starts after most other beans. */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    private void startConsumer(SubscriptionDescriptor descriptor) throws Exception {
        MessageProcessingPipeline<?> pipeline =
                pipelineRegistry.getPipeline(descriptor.getTopic());

        switch (descriptor.getMode()) {
            case STREAMING -> {
                SolaceSessionWrapper session = clientFactory.createSession();
                StreamingConsumer consumer =
                        new StreamingConsumer(session, descriptor, pipeline);
                consumer.start();
                activeConsumers.add(consumer);
                log.info("StreamingConsumer started for topic={}", descriptor.getTopic());
            }
            case SNAPSHOT -> {
                SolaceSessionWrapper session = clientFactory.createSession();
                SnapshotConsumer consumer = new SnapshotConsumer(
                        session, descriptor, pipeline, properties.getSnapshotTimeoutMs());
                // Snapshot runs asynchronously so the manager start() does not block.
                Thread snapshotThread = new Thread(() -> {
                    try {
                        consumer.start();
                    } catch (Exception e) {
                        log.error("SnapshotConsumer error for topic={}: {}",
                                descriptor.getTopic(), e.getMessage(), e);
                    } finally {
                        try {
                            consumer.close();
                        } catch (Exception ex) {
                            log.warn("Error closing SnapshotConsumer", ex);
                        }
                    }
                }, "snapshot-consumer-" + descriptor.getTopic());
                snapshotThread.setDaemon(true);
                snapshotThread.start();
                activeConsumers.add(consumer);
                log.info("SnapshotConsumer started (async) for topic={}", descriptor.getTopic());
            }
            case TRIGGER_THEN_CONSUME -> {
                TriggerThenConsumeService service = new TriggerThenConsumeService(
                        clientFactory, descriptor, pipeline, properties.getTriggerTimeoutMs());
                Thread triggerThread = new Thread(() -> {
                    try {
                        service.start();
                    } catch (Exception e) {
                        log.error("TriggerThenConsumeService error for topic={}: {}",
                                descriptor.getTopic(), e.getMessage(), e);
                    } finally {
                        try {
                            service.close();
                        } catch (Exception ex) {
                            log.warn("Error closing TriggerThenConsumeService", ex);
                        }
                    }
                }, "trigger-consume-" + descriptor.getTopic());
                triggerThread.setDaemon(true);
                triggerThread.start();
                activeConsumers.add(service);
                log.info("TriggerThenConsumeService started (async) for topic={}", descriptor.getTopic());
            }
        }
    }
}
