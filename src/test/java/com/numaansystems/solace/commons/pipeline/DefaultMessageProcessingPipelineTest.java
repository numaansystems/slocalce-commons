package com.numaansystems.solace.commons.pipeline;

import com.numaansystems.solace.commons.model.SolaceMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Destination;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

/**
 * Unit tests for {@link DefaultMessageProcessingPipeline} (built via
 * {@link PipelineBuilder}).
 */
@ExtendWith(MockitoExtension.class)
class DefaultMessageProcessingPipelineTest {

    @Mock
    private BytesXMLMessage rawMessage;

    @Mock
    private Destination destination;

    // -------------------------------------------------------------------------
    // Basic flow
    // -------------------------------------------------------------------------

    @Test
    void pipeline_shouldDecodeFilterEnrichAndHandle() {
        given(rawMessage.getDestination()).willReturn(destination);
        given(destination.getName()).willReturn("test/topic");
        given(rawMessage.getApplicationMessageId()).willReturn("msg-1");
        given(rawMessage.getCorrelationId()).willReturn(null);
        given(rawMessage.getProperties()).willReturn(null);

        AtomicReference<String> captured = new AtomicReference<>();

        MessageProcessingPipeline<String> pipeline = PipelineBuilder.<String>create()
                .decoder(msg -> "decoded-payload")
                .filter(p -> p.startsWith("decoded"))
                .enricher((p, msg) -> p + "-enriched")
                .handler((payload, message) -> captured.set(payload))
                .build();

        pipeline.process(rawMessage);

        assertThat(captured.get()).isEqualTo("decoded-payload-enriched");
    }

    @Test
    void pipeline_shouldDiscardWhenFilterRejects() {
        List<String> received = new ArrayList<>();

        MessageProcessingPipeline<String> pipeline = PipelineBuilder.<String>create()
                .decoder(msg -> "hello")
                .filter(p -> false)          // always reject
                .handler((p, msg) -> received.add(p))
                .build();

        pipeline.process(rawMessage);

        assertThat(received).isEmpty();
    }

    @Test
    void pipeline_shouldDiscardWhenDecoderReturnsNull() {
        List<String> received = new ArrayList<>();

        MessageProcessingPipeline<String> pipeline = PipelineBuilder.<String>create()
                .decoder(msg -> null)        // null means discard
                .handler((p, msg) -> received.add(p))
                .build();

        pipeline.process(rawMessage);

        assertThat(received).isEmpty();
    }

    @Test
    void pipeline_shouldContinueWhenDecoderThrows() {
        // The pipeline should NOT propagate the exception; the handler should NOT be called.
        List<String> received = new ArrayList<>();

        MessageProcessingPipeline<String> pipeline = PipelineBuilder.<String>create()
                .decoder(msg -> { throw new RuntimeException("bad data"); })
                .handler((p, msg) -> received.add(p))
                .build();

        // Must not throw
        pipeline.process(rawMessage);

        assertThat(received).isEmpty();
    }

    @Test
    void pipeline_shouldRunMultipleFiltersInOrder() {
        List<String> filterLog = new ArrayList<>();

        MessageProcessingPipeline<String> pipeline = PipelineBuilder.<String>create()
                .decoder(msg -> "payload")
                .filter(p -> { filterLog.add("f1-accept"); return true; })
                .filter(p -> { filterLog.add("f2-reject"); return false; })
                .filter(p -> { filterLog.add("f3-never"); return true; })
                .handler((p, msg) -> filterLog.add("handler"))
                .build();

        pipeline.process(rawMessage);

        assertThat(filterLog).containsExactly("f1-accept", "f2-reject");
    }

    @Test
    void pipeline_shouldRunMultipleEnrichersInOrder() {
        given(rawMessage.getDestination()).willReturn(null);
        given(rawMessage.getApplicationMessageId()).willReturn(null);
        given(rawMessage.getCorrelationId()).willReturn(null);
        given(rawMessage.getProperties()).willReturn(null);

        AtomicReference<String> captured = new AtomicReference<>();

        MessageProcessingPipeline<String> pipeline = PipelineBuilder.<String>create()
                .decoder(msg -> "A")
                .enricher((p, msg) -> p + "B")
                .enricher((p, msg) -> p + "C")
                .handler((p, msg) -> captured.set(p))
                .build();

        pipeline.process(rawMessage);

        assertThat(captured.get()).isEqualTo("ABC");
    }

    @Test
    void pipeline_shouldPopulateMessageEnvelope() {
        given(rawMessage.getDestination()).willReturn(destination);
        given(destination.getName()).willReturn("my/topic");
        given(rawMessage.getApplicationMessageId()).willReturn("app-id-42");
        given(rawMessage.getCorrelationId()).willReturn("corr-99");
        given(rawMessage.getProperties()).willReturn(null);

        AtomicReference<SolaceMessage<String>> capturedMsg = new AtomicReference<>();

        MessageProcessingPipeline<String> pipeline = PipelineBuilder.<String>create()
                .decoder(msg -> "payload")
                .handler((p, msg) -> capturedMsg.set(msg))
                .build();

        pipeline.process(rawMessage);

        SolaceMessage<String> envelope = capturedMsg.get();
        assertThat(envelope.getTopic()).isEqualTo("my/topic");
        assertThat(envelope.getMessageId()).isEqualTo("app-id-42");
        assertThat(envelope.getCorrelationId()).isEqualTo("corr-99");
        assertThat(envelope.getReceivedAt()).isNotNull();
    }

    @Test
    void pipeline_shouldUseNoOpDecoderWhenNoneProvided() {
        // PipelineBuilder without .decoder() should still compile and run via NoOpDecoder.
        // We just verify that no NPE is thrown (NoOpDecoder handles TextMessage,
        // BytesMessage, and the fallback dump() path – we mock the fallback here).
        given(rawMessage.dump()).willReturn("dumped-content");

        AtomicReference<String> captured = new AtomicReference<>();

        @SuppressWarnings("unchecked")
        MessageProcessingPipeline<String> pipeline = (MessageProcessingPipeline<String>)
                PipelineBuilder.<String>create()
                        .handler((p, msg) -> captured.set(p))
                        .build();

        given(rawMessage.getDestination()).willReturn(null);
        given(rawMessage.getApplicationMessageId()).willReturn(null);
        given(rawMessage.getCorrelationId()).willReturn(null);
        given(rawMessage.getProperties()).willReturn(null);

        pipeline.process(rawMessage);

        assertThat(captured.get()).isEqualTo("dumped-content");
    }
}
