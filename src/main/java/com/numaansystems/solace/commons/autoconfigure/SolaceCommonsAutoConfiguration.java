package com.numaansystems.solace.commons.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.numaansystems.solace.commons.listener.DefaultTopicExtractor;
import com.numaansystems.solace.commons.listener.TopicExtractor;
import com.numaansystems.solace.commons.parser.JsonMessageParser;
import com.numaansystems.solace.commons.pipeline.MessagePipeline;
import com.numaansystems.solace.commons.pipeline.PipelineFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;

import java.util.Map;
import java.util.function.Consumer;

/**
 * Spring Boot auto-configuration for the Solace Commons listener library.
 *
 * <p>Registers:
 * <ul>
 *   <li>A {@link Consumer Consumer&lt;Message&lt;byte[]&gt;&gt;} named
 *       {@code solaceMessageConsumer} — wire it to a Spring Cloud Stream binding.</li>
 *   <li>A default {@link JsonMessageParser} bean (if no custom parser is present).</li>
 *   <li>A default {@link DefaultTopicExtractor} bean.</li>
 * </ul>
 *
 * <p>Enable with {@code solace.commons.enabled=true} (default).
 */
@AutoConfiguration
@EnableConfigurationProperties(SolaceCommonsProperties.class)
@ConditionalOnProperty(prefix = "solace.commons", name = "enabled", matchIfMissing = true)
public class SolaceCommonsAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(SolaceCommonsAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean
    public TopicExtractor topicExtractor() {
        return new DefaultTopicExtractor();
    }

    @Bean
    @ConditionalOnMissingBean(name = "jsonMessageParser")
    public JsonMessageParser jsonMessageParser(ObjectMapper objectMapper) {
        return new JsonMessageParser(objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    public PipelineFactory pipelineFactory(SolaceCommonsProperties properties,
                                           ApplicationContext applicationContext) {
        return new PipelineFactory(properties, applicationContext);
    }

    /**
     * The main Solace message consumer function. Bind it in {@code application.yml}:
     *
     * <pre>
     * spring:
     *   cloud:
     *     stream:
     *       function:
     *         definition: solaceMessageConsumer
     *       bindings:
     *         solaceMessageConsumer-in-0:
     *           destination: orders/created
     * </pre>
     */
    @Bean
    @ConditionalOnMissingBean(name = "solaceMessageConsumer")
    public Consumer<Message<byte[]>> solaceMessageConsumer(PipelineFactory pipelineFactory,
                                                           TopicExtractor topicExtractor) {
        Map<String, MessagePipeline<?>> pipelines = pipelineFactory.buildAll();
        log.info("Solace Commons consumer initialised with {} topic pipeline(s)", pipelines.size());

        return message -> {
            String topic = topicExtractor.extract(message.getHeaders());
            if (topic == null) {
                log.warn("Cannot determine topic for message; dropping. Headers: {}", message.getHeaders().keySet());
                return;
            }

            MessagePipeline<?> pipeline = pipelines.get(topic);
            if (pipeline == null) {
                log.warn("No pipeline configured for topic '{}'; dropping message", topic);
                return;
            }

            log.debug("Routing message from topic '{}' through pipeline", topic);
            pipeline.process(message.getPayload(), message.getHeaders(), topic);
        };
    }
}
