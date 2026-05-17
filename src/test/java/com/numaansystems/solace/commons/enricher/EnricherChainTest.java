package com.numaansystems.solace.commons.enricher;

import com.numaansystems.solace.commons.model.ProcessedMessage;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.MessageHeaders;

import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EnricherChainTest {

    private ProcessedMessage<String> newMessage(String payload) {
        return ProcessedMessage.of(payload, new MessageHeaders(new HashMap<>()), "test/topic");
    }

    @Test
    void enrichers_areAppliedInOrder() {
        MessageEnricher<String> enricher1 = msg -> msg.withMetadata("step", "first");
        MessageEnricher<String> enricher2 = msg -> msg.withMetadata("step", "second");

        EnricherChain<String> chain = new EnricherChain<>(List.of(enricher1, enricher2));
        ProcessedMessage<String> result = chain.enrich(newMessage("hello"));

        // Last enricher wins for same key
        assertThat(result.getMetadata()).containsEntry("step", "second");
    }

    @Test
    void enrichers_accumulateDistinctMetadata() {
        MessageEnricher<String> enricher1 = msg -> msg.withMetadata("a", "1");
        MessageEnricher<String> enricher2 = msg -> msg.withMetadata("b", "2");

        EnricherChain<String> chain = new EnricherChain<>(List.of(enricher1, enricher2));
        ProcessedMessage<String> result = chain.enrich(newMessage("hello"));

        assertThat(result.getMetadata()).containsEntry("a", "1").containsEntry("b", "2");
    }

    @Test
    void emptyChain_returnsOriginalMessage() {
        EnricherChain<String> chain = new EnricherChain<>(List.of());
        ProcessedMessage<String> original = newMessage("hello");
        ProcessedMessage<String> result = chain.enrich(original);
        assertThat(result).isSameAs(original);
    }

    @Test
    void enricher_thatThrows_isSkippedAndChainContinues() {
        MessageEnricher<String> failingEnricher = msg -> { throw new RuntimeException("boom"); };
        MessageEnricher<String> goodEnricher = msg -> msg.withMetadata("ok", "yes");

        EnricherChain<String> chain = new EnricherChain<>(List.of(failingEnricher, goodEnricher));
        ProcessedMessage<String> result = chain.enrich(newMessage("hello"));

        // Chain should continue past the failing enricher
        assertThat(result.getMetadata()).containsEntry("ok", "yes");
    }
}
