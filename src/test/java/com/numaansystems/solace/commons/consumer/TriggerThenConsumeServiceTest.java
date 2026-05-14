package com.numaansystems.solace.commons.consumer;

import com.numaansystems.solace.commons.config.ConsumerMode;
import com.numaansystems.solace.commons.config.SubscriptionDescriptor;
import com.numaansystems.solace.commons.connection.SolaceClientFactory;
import com.numaansystems.solace.commons.connection.SolaceSessionWrapper;
import com.numaansystems.solace.commons.pipeline.MessageProcessingPipeline;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for {@link TriggerThenConsumeService} correlation logic.
 *
 * <p>These tests verify the service wiring and correlation-ID matching using
 * mocked Solace sessions – no broker required.
 */
@ExtendWith(MockitoExtension.class)
class TriggerThenConsumeServiceTest {

    @Mock
    private SolaceClientFactory clientFactory;

    @Mock
    private SolaceSessionWrapper sessionWrapper;

    @Mock
    private XMLMessageConsumer xmlMessageConsumer;

    @Mock
    private XMLMessageProducer xmlMessageProducer;

    @Mock
    private BytesXMLMessage correlatedMessage;

    @Mock
    private BytesXMLMessage nonCorrelatedMessage;

    private final List<BytesXMLMessage> processedMessages = new ArrayList<>();

    private MessageProcessingPipeline<BytesXMLMessage> capturingPipeline;
    private SubscriptionDescriptor descriptor;

    @BeforeEach
    void setUp() throws JCSMPException {
        capturingPipeline = rawMessage -> processedMessages.add(rawMessage);

        descriptor = new SubscriptionDescriptor();
        descriptor.setTopic("response/topic");
        descriptor.setMode(ConsumerMode.TRIGGER_THEN_CONSUME);
        descriptor.setTriggerTopic("request/topic");
        descriptor.setTriggerPayload("{\"action\":\"fetch\"}");

        given(clientFactory.createSession()).willReturn(sessionWrapper);
        // TriggerPublisher calls createProducer(null), so use nullable() matcher.
        given(sessionWrapper.createProducer(
                nullable(JCSMPStreamingPublishCorrelatingEventHandler.class)))
                .willReturn(xmlMessageProducer);
    }

    @Test
    void start_shouldSubscribeToResponseTopic() throws Exception {
        TriggerThenConsumeService service = new TriggerThenConsumeService(
                clientFactory, descriptor, capturingPipeline, 100);

        // Fire a null-correlationId message immediately when the consumer is created.
        given(sessionWrapper.createConsumer(any(XMLMessageListener.class)))
                .willAnswer(inv -> {
                    XMLMessageListener listener = inv.getArgument(0);
                    given(correlatedMessage.getCorrelationId()).willReturn(null);
                    listener.onReceive(correlatedMessage);
                    return xmlMessageConsumer;
                });

        service.start();

        verify(sessionWrapper).connect();
        verify(sessionWrapper).addSubscription("response/topic");
        assertThat(processedMessages).containsExactly(correlatedMessage);
    }

    @Test
    void start_shouldIgnoreMessagesWithNonMatchingCorrelationId() throws Exception {
        TriggerThenConsumeService service = new TriggerThenConsumeService(
                clientFactory, descriptor, capturingPipeline, 200);

        given(sessionWrapper.createConsumer(any(XMLMessageListener.class)))
                .willAnswer(inv -> {
                    XMLMessageListener listener = inv.getArgument(0);
                    // This message carries a random ID that will not match the generated UUID.
                    given(nonCorrelatedMessage.getCorrelationId())
                            .willReturn("completely-different-id");
                    listener.onReceive(nonCorrelatedMessage);
                    // Also send a null-correlationId message to unblock the latch.
                    given(correlatedMessage.getCorrelationId()).willReturn(null);
                    listener.onReceive(correlatedMessage);
                    return xmlMessageConsumer;
                });

        service.start();

        // Only the null-correlationId message should be processed.
        assertThat(processedMessages).doesNotContain(nonCorrelatedMessage);
        assertThat(processedMessages).contains(correlatedMessage);
    }

    @Test
    void start_shouldTimeOutGracefullyWhenNoResponseArrives() throws Exception {
        // Consumer never fires any message – the service should return after timeout.
        given(sessionWrapper.createConsumer(any(XMLMessageListener.class)))
                .willReturn(xmlMessageConsumer);

        TriggerThenConsumeService service = new TriggerThenConsumeService(
                clientFactory, descriptor, capturingPipeline, 50 /* short timeout */);

        long before = System.currentTimeMillis();
        service.start();
        long elapsed = System.currentTimeMillis() - before;

        assertThat(processedMessages).isEmpty();
        // Should have waited at least most of the timeout duration.
        assertThat(elapsed).isGreaterThanOrEqualTo(40L);
    }
}
