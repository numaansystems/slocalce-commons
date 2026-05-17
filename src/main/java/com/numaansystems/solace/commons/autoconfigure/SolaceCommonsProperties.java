package com.numaansystems.solace.commons.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Configuration properties for the Solace Commons listener library.
 *
 * <pre>
 * solace:
 *   commons:
 *     enabled: true
 *     topics:
 *       "orders/created":
 *         parser: orderParser          # bean name
 *         enrichers:
 *           - customerEnricher
 *           - inventoryEnricher
 *         sink-publish-enabled: true
 *         sink-publish-target: processedOrders-out-0
 *         sink-database-enabled: true
 *     sinks:
 *       publish:
 *         enabled: false
 *         target: defaultOutput-out-0
 *       database:
 *         enabled: false
 * </pre>
 */
@ConfigurationProperties(prefix = "solace.commons")
public class SolaceCommonsProperties {

    /** Global enable/disable flag for the library. */
    private boolean enabled = true;

    /**
     * Per-topic pipeline configuration. Map key is the Solace topic name.
     */
    private Map<String, TopicConfig> topics = new LinkedHashMap<>();

    /** Global sink defaults (can be overridden per topic). */
    private SinkConfig sinks = new SinkConfig();

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public Map<String, TopicConfig> getTopics() { return topics; }
    public void setTopics(Map<String, TopicConfig> topics) { this.topics = topics; }

    public SinkConfig getSinks() { return sinks; }
    public void setSinks(SinkConfig sinks) { this.sinks = sinks; }

    // -------------------------------------------------------------------------

    /** Per-topic pipeline configuration. */
    public static class TopicConfig {

        /** Bean name of the {@link com.numaansystems.solace.commons.parser.MessageParser} to use. */
        private String parser;

        /** Ordered list of {@link com.numaansystems.solace.commons.enricher.MessageEnricher} bean names. */
        private List<String> enrichers = new ArrayList<>();

        /** Ordered list of additional {@link com.numaansystems.solace.commons.sink.MessageSink} bean names. */
        private List<String> sinks = new ArrayList<>();

        /** Per-topic override: enable downstream publish sink. Null = use global setting. */
        private Boolean sinkPublishEnabled;

        /** Per-topic override: target binding for publish sink. Null = use global setting. */
        private String sinkPublishTarget;

        /** Per-topic override: enable database sink. Null = use global setting. */
        private Boolean sinkDatabaseEnabled;

        public String getParser() { return parser; }
        public void setParser(String parser) { this.parser = parser; }

        public List<String> getEnrichers() { return enrichers; }
        public void setEnrichers(List<String> enrichers) { this.enrichers = enrichers; }

        public List<String> getSinks() { return sinks; }
        public void setSinks(List<String> sinks) { this.sinks = sinks; }

        public Boolean isSinkPublishEnabled() { return sinkPublishEnabled; }
        public void setSinkPublishEnabled(Boolean sinkPublishEnabled) { this.sinkPublishEnabled = sinkPublishEnabled; }

        public String getSinkPublishTarget() { return sinkPublishTarget; }
        public void setSinkPublishTarget(String sinkPublishTarget) { this.sinkPublishTarget = sinkPublishTarget; }

        public Boolean isSinkDatabaseEnabled() { return sinkDatabaseEnabled; }
        public void setSinkDatabaseEnabled(Boolean sinkDatabaseEnabled) { this.sinkDatabaseEnabled = sinkDatabaseEnabled; }
    }

    // -------------------------------------------------------------------------

    /** Global sink defaults. */
    public static class SinkConfig {

        private PublishConfig publish = new PublishConfig();
        private DatabaseConfig database = new DatabaseConfig();

        public PublishConfig getPublish() { return publish; }
        public void setPublish(PublishConfig publish) { this.publish = publish; }

        public DatabaseConfig getDatabase() { return database; }
        public void setDatabase(DatabaseConfig database) { this.database = database; }
    }

    /** Downstream publish sink configuration. */
    public static class PublishConfig {
        private boolean enabled = false;
        /** Spring Cloud Stream output binding name (e.g. {@code myOutput-out-0}). */
        private String target;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }

        public String getTarget() { return target; }
        public void setTarget(String target) { this.target = target; }
    }

    /** Database sink configuration. */
    public static class DatabaseConfig {
        private boolean enabled = false;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
    }
}
