package com.numaansystems.solace.commons.pipeline;

import com.numaansystems.solace.commons.model.SolaceMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple {@link MessageHandler} that logs every received message at INFO
 * level.
 *
 * <p>This is the default handler used by the auto-configured
 * {@code CommonsConsumerManager} when no application-specific handler is
 * registered for a topic.
 *
 * @param <T> the payload type
 */
public class LoggingMessageHandler<T> implements MessageHandler<T> {

    private static final Logger log = LoggerFactory.getLogger(LoggingMessageHandler.class);

    @Override
    public void handle(T payload, SolaceMessage<T> message) {
        log.info("Received message [topic={}, id={}, correlationId={}]: {}",
                message.getTopic(),
                message.getMessageId(),
                message.getCorrelationId(),
                payload);
    }
}
