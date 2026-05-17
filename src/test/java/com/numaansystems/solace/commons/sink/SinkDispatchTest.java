package com.numaansystems.solace.commons.sink;

import com.numaansystems.solace.commons.model.ProcessedMessage;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.MessageHeaders;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SinkDispatchTest {

    private ProcessedMessage<String> newMessage(String payload) {
        return ProcessedMessage.of(payload, new MessageHeaders(new HashMap<>()), "test/topic");
    }

    @Test
    void downstreamPublishSink_sendsViaStreamBridge() {
        StreamBridge streamBridge = mock(StreamBridge.class);
        DownstreamPublishSink<String> sink = new DownstreamPublishSink<>(streamBridge, "myOutput-out-0");

        sink.send(newMessage("payload"));

        verify(streamBridge).send(eq("myOutput-out-0"), any());
    }

    @Test
    void databaseSink_delegatesToHook() {
        AtomicBoolean saved = new AtomicBoolean(false);
        DatabasePersistenceHook<String> hook = message -> saved.set(true);
        DatabaseSink<String> sink = new DatabaseSink<>(hook);

        sink.send(newMessage("payload"));

        assertThat(saved.get()).isTrue();
    }

    @Test
    void databaseSink_passesMessageToHook() {
        ProcessedMessage<String>[] holder = new ProcessedMessage[1];
        DatabasePersistenceHook<String> hook = message -> holder[0] = message;
        DatabaseSink<String> sink = new DatabaseSink<>(hook);

        ProcessedMessage<String> original = newMessage("hello");
        sink.send(original);

        assertThat(holder[0]).isSameAs(original);
    }
}
