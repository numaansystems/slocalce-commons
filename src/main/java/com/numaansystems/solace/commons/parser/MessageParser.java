package com.numaansystems.solace.commons.parser;

import org.springframework.messaging.MessageHeaders;

/**
 * Strategy interface for parsing raw Solace message bytes into a typed object.
 * <p>
 * Implementations are registered as Spring beans. The {@link ParserRegistry}
 * resolves the correct parser per topic based on configuration.
 *
 * @param <T> the parsed output type
 */
public interface MessageParser<T> {

    /**
     * Parse the raw byte payload into a typed message object.
     *
     * @param payload the raw message bytes
     * @param headers the Spring message headers (from the Solace binder)
     * @return the parsed object; never {@code null}
     * @throws Exception if parsing fails
     */
    T parse(byte[] payload, MessageHeaders headers) throws Exception;
}
