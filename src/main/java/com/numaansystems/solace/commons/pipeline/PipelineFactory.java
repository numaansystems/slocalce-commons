package com.numaansystems.solace.commons.pipeline;

import com.numaansystems.solace.commons.autoconfigure.SolaceCommonsProperties;
import com.numaansystems.solace.commons.enricher.EnricherChain;
import com.numaansystems.solace.commons.enricher.MessageEnricher;
import com.numaansystems.solace.commons.parser.MessageParser;
import com.numaansystems.solace.commons.sink.DatabasePersistenceHook;
import com.numaansystems.solace.commons.sink.DatabaseSink;
import com.numaansystems.solace.commons.sink.DownstreamPublishSink;
import com.numaansystems.solace.commons.sink.MessageSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ApplicationContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds {@link MessagePipeline} instances from {@link SolaceCommonsProperties} and
 * the Spring {@link ApplicationContext} (to look up parser / enricher beans by name).
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class PipelineFactory {

    private static final Logger log = LoggerFactory.getLogger(PipelineFactory.class);

    private final SolaceCommonsProperties properties;
    private final ApplicationContext applicationContext;

    public PipelineFactory(SolaceCommonsProperties properties, ApplicationContext applicationContext) {
        this.properties = properties;
        this.applicationContext = applicationContext;
    }

    /**
     * Build all configured pipelines, keyed by topic name.
     *
     * @return map of topic → pipeline
     */
    public Map<String, MessagePipeline<?>> buildAll() {
        Map<String, MessagePipeline<?>> pipelines = new HashMap<>();

        for (Map.Entry<String, SolaceCommonsProperties.TopicConfig> entry :
                properties.getTopics().entrySet()) {
            String topic = entry.getKey();
            SolaceCommonsProperties.TopicConfig cfg = entry.getValue();
            try {
                MessagePipeline<?> pipeline = build(topic, cfg);
                pipelines.put(topic, pipeline);
                log.info("Built pipeline for topic '{}'", topic);
            } catch (Exception e) {
                log.error("Failed to build pipeline for topic '{}': {}", topic, e.getMessage(), e);
            }
        }

        return pipelines;
    }

    private MessagePipeline<?> build(String topic, SolaceCommonsProperties.TopicConfig cfg) {
        // 1. Resolve parser
        MessageParser<?> parser = resolveParser(topic, cfg);

        // 2. Resolve enrichers
        List<MessageEnricher<?>> enrichers = resolveEnrichers(cfg);

        // 3. Build sinks
        List<MessageSink<?>> sinks = buildSinks(cfg);

        return new MessagePipeline(parser, new EnricherChain(enrichers), sinks);
    }

    private MessageParser<?> resolveParser(String topic, SolaceCommonsProperties.TopicConfig cfg) {
        String beanName = cfg.getParser();
        if (beanName == null || beanName.isBlank()) {
            // Fallback to the default parser bean if available
            if (applicationContext.containsBean("jsonMessageParser")) {
                return (MessageParser<?>) applicationContext.getBean("jsonMessageParser");
            }
            throw new IllegalStateException(
                    "No parser configured for topic '" + topic + "' and no default 'jsonMessageParser' bean found");
        }
        return applicationContext.getBean(beanName, MessageParser.class);
    }

    private List<MessageEnricher<?>> resolveEnrichers(SolaceCommonsProperties.TopicConfig cfg) {
        List<MessageEnricher<?>> enrichers = new ArrayList<>();
        for (String beanName : cfg.getEnrichers()) {
            if (applicationContext.containsBean(beanName)) {
                enrichers.add(applicationContext.getBean(beanName, MessageEnricher.class));
            } else {
                log.warn("Enricher bean '{}' not found in context; skipping", beanName);
            }
        }
        return enrichers;
    }

    private List<MessageSink<?>> buildSinks(SolaceCommonsProperties.TopicConfig cfg) {
        List<MessageSink<?>> sinks = new ArrayList<>();

        SolaceCommonsProperties.SinkConfig globalSinks = properties.getSinks();

        // Per-topic sink overrides; falls back to global sink config
        boolean publishEnabled = cfg.isSinkPublishEnabled() != null
                ? cfg.isSinkPublishEnabled()
                : globalSinks.getPublish().isEnabled();

        boolean databaseEnabled = cfg.isSinkDatabaseEnabled() != null
                ? cfg.isSinkDatabaseEnabled()
                : globalSinks.getDatabase().isEnabled();

        if (publishEnabled) {
            String target = cfg.getSinkPublishTarget() != null
                    ? cfg.getSinkPublishTarget()
                    : globalSinks.getPublish().getTarget();
            if (target == null || target.isBlank()) {
                log.warn("Publish sink enabled but no target binding configured; skipping publish sink");
            } else {
                StreamBridge streamBridge = applicationContext.getBean(StreamBridge.class);
                sinks.add(new DownstreamPublishSink<>(streamBridge, target));
            }
        }

        if (databaseEnabled) {
            if (applicationContext.getBeanNamesForType(DatabasePersistenceHook.class).length > 0) {
                DatabasePersistenceHook<?> hook = applicationContext.getBean(DatabasePersistenceHook.class);
                sinks.add(new DatabaseSink<>(hook));
            } else {
                log.warn("Database sink enabled but no DatabasePersistenceHook bean found; skipping database sink");
            }
        }

        // Allow extra custom sinks configured by bean name
        for (String sinkBeanName : cfg.getSinks()) {
            if (applicationContext.containsBean(sinkBeanName)) {
                sinks.add(applicationContext.getBean(sinkBeanName, MessageSink.class));
            } else {
                log.warn("Sink bean '{}' not found in context; skipping", sinkBeanName);
            }
        }

        return sinks;
    }
}
