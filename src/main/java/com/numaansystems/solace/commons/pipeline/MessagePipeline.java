package com.numaansystems.solace.commons.pipeline;

import com.numaansystems.solace.commons.enricher.EnricherChain;
import com.numaansystems.solace.commons.model.ProcessedMessage;
import com.numaansystems.solace.commons.parser.MessageParser;
import com.numaansystems.solace.commons.sink.MessageSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.MessageHeaders;
import java.util.List;

/**
 * Represents the processing pipeline for a single topic (or group of topics):
 * <ol>
 *   <li>Parse raw bytes using the configured {@link MessageParser}</li>
 *   <li>Enrich via the ordered {@link EnricherChain}</li>
 *   <li>Dispatch to one or more {@link MessageSink}s</li>
 * </ol>
 *
 * @param <T> the parsed payload type
 */
public class MessagePipeline<T> {

    private static final Logger log = LoggerFactory.getLogger(MessagePipeline.class);

    private final MessageParser<T> parser;
    private final EnricherChain<T> enricherChain;
    private final List<MessageSink<T>> sinks;

    public MessagePipeline(MessageParser<T> parser,
                           EnricherChain<T> enricherChain,
                           List<MessageSink<T>> sinks) {
        this.parser = parser;
        this.enricherChain = enricherChain;
        this.sinks = List.copyOf(sinks);
    }

    /**
     * Execute the full pipeline for one message.
     *
     * @param payload     raw message bytes
     * @param headers     Spring message headers
     * @param sourceTopic the Solace topic this message arrived on
     */
    @SuppressWarnings("unchecked")
    public void process(byte[] payload, MessageHeaders headers, String sourceTopic) {
        T parsed;
        try {
            parsed = parser.parse(payload, headers);
        } catch (Exception e) {
            log.error("Failed to parse message from topic '{}': {}", sourceTopic, e.getMessage(), e);
            return;
        }

        ProcessedMessage<T> message = ProcessedMessage.of(parsed, headers, sourceTopic);
        message = enricherChain.enrich(message);

        for (MessageSink<T> sink : sinks) {
            try {
                sink.send(message);
            } catch (Exception e) {
                log.error("Sink {} failed for topic '{}': {}",
                        sink.getClass().getSimpleName(), sourceTopic, e.getMessage(), e);
            }
        }
    }
}
