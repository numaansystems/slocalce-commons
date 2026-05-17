package com.numaansystems.solace.commons.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.numaansystems.solace.commons.autoconfigure.SolaceCommonsProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.messaging.MessageHeaders;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class ParserSelectionTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    @Test
    void jsonParser_parsesValidJsonBytes() throws Exception {
        JsonMessageParser parser = new JsonMessageParser(objectMapper);
        byte[] json = "{\"key\":\"value\"}".getBytes();
        Map<String, Object> result = parser.parse(json, new MessageHeaders(new HashMap<>()));
        assertThat(result).containsEntry("key", "value");
    }

    @Test
    void parserRegistry_resolvesConfiguredParser() {
        ApplicationContext ctx = mock(ApplicationContext.class);
        JsonMessageParser mockParser = new JsonMessageParser(objectMapper);
        when(ctx.containsBean("myParser")).thenReturn(true);
        when(ctx.getBean(eq("myParser"), eq(MessageParser.class))).thenReturn((MessageParser) mockParser);

        Map<String, String> mapping = Map.of("orders/created", "myParser");
        ParserRegistry registry = new ParserRegistry(ctx, mapping, null);

        MessageParser<?> resolved = registry.resolve("orders/created");
        assertThat(resolved).isSameAs(mockParser);
    }

    @Test
    void parserRegistry_fallsBackToDefaultParser() {
        ApplicationContext ctx = mock(ApplicationContext.class);
        JsonMessageParser defaultParser = new JsonMessageParser(objectMapper);
        when(ctx.containsBean("defaultParser")).thenReturn(true);
        when(ctx.getBean(eq("defaultParser"), eq(MessageParser.class))).thenReturn((MessageParser) defaultParser);

        ParserRegistry registry = new ParserRegistry(ctx, new HashMap<>(), "defaultParser");

        MessageParser<?> resolved = registry.resolve("unknown/topic");
        assertThat(resolved).isSameAs(defaultParser);
    }

    @Test
    void parserRegistry_returnsNullWhenNoParserFound() {
        ApplicationContext ctx = mock(ApplicationContext.class);
        ParserRegistry registry = new ParserRegistry(ctx, new HashMap<>(), null);

        MessageParser<?> resolved = registry.resolve("unknown/topic");
        assertThat(resolved).isNull();
    }
}
