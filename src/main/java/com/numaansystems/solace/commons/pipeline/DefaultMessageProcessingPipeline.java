package com.numaansystems.solace.commons.pipeline;

import com.numaansystems.solace.commons.model.SolaceMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Default implementation of {@link MessageProcessingPipeline}.
 *
 * <p>Exceptions thrown by the decoder, filters, or enrichers are caught and
 * logged so that a single bad message cannot break the consumer loop.
 *
 * <p>Construct via {@link PipelineBuilder}.
 *
 * @param <T> the payload type
 */
public class DefaultMessageProcessingPipeline<T> implements MessageProcessingPipeline<T> {

    private static final Logger log =
            LoggerFactory.getLogger(DefaultMessageProcessingPipeline.class);

    private final MessageDecoder<T> decoder;
    private final List<MessageFilter<T>> filters;
    private final List<MessageEnricher<T>> enrichers;
    private final MessageHandler<T> handler;

    DefaultMessageProcessingPipeline(MessageDecoder<T> decoder,
                                     List<MessageFilter<T>> filters,
                                     List<MessageEnricher<T>> enrichers,
                                     MessageHandler<T> handler) {
        this.decoder = decoder;
        this.filters = Collections.unmodifiableList(filters);
        this.enrichers = Collections.unmodifiableList(enrichers);
        this.handler = handler;
    }

    @Override
    public void process(BytesXMLMessage rawMessage) {
        T payload;
        try {
            payload = decoder.decode(rawMessage);
        } catch (Exception e) {
            log.error("Decoder threw an exception – message discarded", e);
            return;
        }

        if (payload == null) {
            log.debug("Decoder returned null – message discarded");
            return;
        }

        for (MessageFilter<T> filter : filters) {
            try {
                if (!filter.accept(payload)) {
                    log.trace("Message filtered out by {}", filter.getClass().getSimpleName());
                    return;
                }
            } catch (Exception e) {
                log.error("Filter threw an exception – message discarded", e);
                return;
            }
        }

        for (MessageEnricher<T> enricher : enrichers) {
            try {
                payload = enricher.enrich(payload, rawMessage);
                if (payload == null) {
                    log.debug("Enricher returned null – message discarded");
                    return;
                }
            } catch (Exception e) {
                log.error("Enricher threw an exception – message discarded", e);
                return;
            }
        }

        SolaceMessage<T> message = buildEnvelope(payload, rawMessage);
        try {
            handler.handle(payload, message);
        } catch (Exception e) {
            log.error("Handler threw an exception for message on topic '{}'",
                    message.getTopic(), e);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private SolaceMessage<T> buildEnvelope(T payload, BytesXMLMessage raw) {
        String topic = raw.getDestination() != null ? raw.getDestination().getName() : null;
        String messageId = raw.getApplicationMessageId();
        String correlationId = raw.getCorrelationId();

        Map<String, Object> userProps = new HashMap<>();
        SDTMap sdtMap = raw.getProperties();
        if (sdtMap != null) {
            for (String key : sdtMap.keySet()) {
                try {
                    userProps.put(key, sdtMap.get(key));
                } catch (SDTException e) {
                    log.trace("Could not read user property '{}': {}", key, e.getMessage());
                }
            }
        }

        return new SolaceMessage<>(payload, topic, messageId, correlationId, userProps);
    }
}
