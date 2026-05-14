package com.numaansystems.solace.commons.autoconfigure;

import com.numaansystems.solace.commons.pipeline.LoggingMessageHandler;
import com.numaansystems.solace.commons.pipeline.MessageProcessingPipeline;
import com.numaansystems.solace.commons.pipeline.NoOpDecoder;
import com.numaansystems.solace.commons.pipeline.PipelineBuilder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Central registry that maps Solace topic strings to
 * {@link MessageProcessingPipeline} instances.
 *
 * <p>Client applications register their custom pipelines during Spring
 * context initialisation (e.g. in a {@code @PostConstruct} method or a
 * {@code @Bean} definition):
 *
 * <pre>{@code
 * @Autowired PipelineRegistry registry;
 *
 * @PostConstruct
 * void setup() {
 *     registry.register("market/prices/AAPL",
 *         PipelineBuilder.<PriceDto>create()
 *             .decoder(new JsonDecoder<>(PriceDto.class, objectMapper))
 *             .filter(p -> p.getPrice() > 0)
 *             .handler((p, msg) -> priceService.update(p))
 *             .build());
 * }
 * }</pre>
 *
 * <p>Topics without a registered pipeline fall back to a default pipeline that
 * logs every message at INFO level.
 */
public class PipelineRegistry {

    private final Map<String, MessageProcessingPipeline<?>> pipelines = new ConcurrentHashMap<>();

    /**
     * Register a pipeline for the given Solace topic.
     *
     * @param topic    the exact topic string (must match what is configured in
     *                 {@code solace.commons.subscriptions[*].topic})
     * @param pipeline the processing pipeline to invoke for messages on this topic
     * @param <T>      payload type
     */
    public <T> void register(String topic, MessageProcessingPipeline<T> pipeline) {
        pipelines.put(topic, pipeline);
    }

    /**
     * Look up the pipeline for a topic.  Falls back to a logging no-op pipeline
     * if no specific pipeline has been registered.
     *
     * @param topic the subscription topic
     * @return the registered pipeline, or the default logging pipeline
     */
    public MessageProcessingPipeline<?> getPipeline(String topic) {
        return pipelines.computeIfAbsent(topic, t ->
                PipelineBuilder.<String>create()
                        .decoder(new NoOpDecoder())
                        .handler(new LoggingMessageHandler<>())
                        .build());
    }

    /** Returns a snapshot of all currently registered topic-to-pipeline entries. */
    public Map<String, MessageProcessingPipeline<?>> getAll() {
        return Map.copyOf(pipelines);
    }
}
