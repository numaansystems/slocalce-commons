package com.numaansystems.solace.commons.sink;

import com.numaansystems.solace.commons.model.ProcessedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.support.MessageBuilder;

/**
 * {@link MessageSink} that publishes the processed message payload to a
 * downstream Spring Cloud Stream output binding via {@link StreamBridge}.
 *
 * <p>Configure the target binding name in:
 * {@code solace.commons.sinks.publish.target}
 *
 * @param <T> the payload type
 */
public class DownstreamPublishSink<T> implements MessageSink<T> {

    private static final Logger log = LoggerFactory.getLogger(DownstreamPublishSink.class);

    private final StreamBridge streamBridge;
    private final String targetBinding;

    public DownstreamPublishSink(StreamBridge streamBridge, String targetBinding) {
        this.streamBridge = streamBridge;
        this.targetBinding = targetBinding;
    }

    @Override
    public void send(ProcessedMessage<T> message) {
        log.debug("Publishing message from topic '{}' to binding '{}'",
                message.getSourceTopic(), targetBinding);
        streamBridge.send(targetBinding,
                MessageBuilder.withPayload(message.getPayload())
                        .copyHeaders(message.getMetadata())
                        .setHeader("solace_sourceTopic", message.getSourceTopic())
                        .build());
    }
}
