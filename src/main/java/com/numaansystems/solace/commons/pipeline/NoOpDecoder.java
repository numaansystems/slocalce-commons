package com.numaansystems.solace.commons.pipeline;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.TextMessage;

import java.nio.charset.StandardCharsets;

/**
 * A pass-through {@link MessageDecoder} that extracts the message body as a
 * plain UTF-8 {@link String}.
 *
 * <ul>
 *   <li>{@link TextMessage} → returns {@link TextMessage#getText()}</li>
 *   <li>{@link BytesMessage} → decodes the byte array as UTF-8</li>
 *   <li>Other types → falls back to {@link BytesXMLMessage#dump()}</li>
 * </ul>
 */
public class NoOpDecoder implements MessageDecoder<String> {

    @Override
    public String decode(BytesXMLMessage rawMessage) {
        if (rawMessage instanceof TextMessage textMessage) {
            return textMessage.getText();
        }
        if (rawMessage instanceof BytesMessage bytesMessage) {
            byte[] data = new byte[bytesMessage.getContentLength()];
            bytesMessage.readBytes(data);
            return new String(data, StandardCharsets.UTF_8);
        }
        return rawMessage.dump();
    }
}
