package com.numaansystems.solace.commons.pipeline;

import java.util.ArrayList;
import java.util.List;

/**
 * Fluent builder for {@link MessageProcessingPipeline}.
 *
 * <p>Sensible defaults are applied when a step is omitted:
 * <ul>
 *   <li>Decoder – {@link NoOpDecoder} (returns payload as UTF-8 {@code String})</li>
 *   <li>Handler – no-op (messages are silently discarded after enrichment)</li>
 * </ul>
 *
 * <p>Example:
 * <pre>{@code
 * MessageProcessingPipeline<MyDto> pipeline = PipelineBuilder.<MyDto>create()
 *     .decoder(new JsonDecoder<>(MyDto.class, objectMapper))
 *     .filter(dto -> dto.getPrice() > 0)
 *     .enricher((dto, raw) -> { dto.setReceivedTopic(raw.getDestination().getName()); return dto; })
 *     .handler((dto, msg) -> myService.process(dto))
 *     .build();
 * }</pre>
 *
 * @param <T> the payload type
 */
public final class PipelineBuilder<T> {

    private MessageDecoder<T> decoder;
    private final List<MessageFilter<T>> filters = new ArrayList<>();
    private final List<MessageEnricher<T>> enrichers = new ArrayList<>();
    private MessageHandler<T> handler;

    private PipelineBuilder() {}

    /** Create a new builder for payload type {@code T}. */
    public static <T> PipelineBuilder<T> create() {
        return new PipelineBuilder<>();
    }

    /** Set the message decoder (mandatory – a {@link NoOpDecoder} is used if omitted). */
    public PipelineBuilder<T> decoder(MessageDecoder<T> decoder) {
        this.decoder = decoder;
        return this;
    }

    /** Add a filter; messages rejected by any filter are discarded. */
    public PipelineBuilder<T> filter(MessageFilter<T> filter) {
        this.filters.add(filter);
        return this;
    }

    /** Add an enricher; enrichers run in the order they are added. */
    public PipelineBuilder<T> enricher(MessageEnricher<T> enricher) {
        this.enrichers.add(enricher);
        return this;
    }

    /** Set the final handler that receives the processed payload. */
    public PipelineBuilder<T> handler(MessageHandler<T> handler) {
        this.handler = handler;
        return this;
    }

    /**
     * Build and return the configured pipeline.
     *
     * @throws IllegalStateException if the handler has not been set
     */
    @SuppressWarnings("unchecked")
    public MessageProcessingPipeline<T> build() {
        MessageDecoder<T> resolvedDecoder =
                decoder != null ? decoder : (MessageDecoder<T>) new NoOpDecoder();
        MessageHandler<T> resolvedHandler =
                handler != null ? handler : (payload, message) -> {};
        return new DefaultMessageProcessingPipeline<>(
                resolvedDecoder,
                new ArrayList<>(filters),
                new ArrayList<>(enrichers),
                resolvedHandler);
    }
}
