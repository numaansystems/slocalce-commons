package com.numaansystems.solace.commons.pipeline;

import com.solacesystems.jcsmp.BytesXMLMessage;

/**
 * Converts a raw Solace {@link BytesXMLMessage} into a typed payload object.
 *
 * <p>Implement this interface to support custom serialization formats (JSON,
 * Avro, Protobuf, plain text, …).
 *
 * @param <T> the decoded payload type
 */
@FunctionalInterface
public interface MessageDecoder<T> {

    /**
     * Decode the raw message into a typed payload.
     *
     * @param rawMessage the raw Solace message
     * @return decoded payload, or {@code null} to discard this message
     * @throws Exception if decoding fails
     */
    T decode(BytesXMLMessage rawMessage) throws Exception;
}
