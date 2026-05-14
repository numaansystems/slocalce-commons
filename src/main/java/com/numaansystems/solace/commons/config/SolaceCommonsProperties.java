package com.numaansystems.solace.commons.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Spring Boot configuration properties for the Solace Commons library.
 *
 * <p>All settings live under the {@code solace.commons} prefix. Example
 * {@code application.yml}:
 *
 * <pre>
 * solace:
 *   commons:
 *     enabled: true
 *     connection:
 *       host: tcp://localhost:55555
 *       vpn: default
 *       username: admin
 *       password: admin
 *       client-name: my-service
 *     reconnect-retries: 3
 *     reconnect-retry-wait-ms: 3000
 *     snapshot-timeout-ms: 10000
 *     trigger-timeout-ms: 30000
 *     subscriptions:
 *       - topic: my/streaming/topic
 *         mode: STREAMING
 *       - topic: my/snapshot/topic
 *         mode: SNAPSHOT
 *         stop-on-first-snapshot: true
 * </pre>
 */
@ConfigurationProperties(prefix = "solace.commons")
public class SolaceCommonsProperties {

    /** Master switch – set to {@code false} to disable all consumers without removing config. */
    private boolean enabled = true;

    /** Solace PubSub+ broker connection settings. */
    private Connection connection = new Connection();

    /** Topic subscriptions to create on startup. */
    private List<SubscriptionDescriptor> subscriptions = new ArrayList<>();

    /** Number of reconnect retries before giving up. */
    private int reconnectRetries = 3;

    /** Milliseconds to wait between reconnect attempts. */
    private int reconnectRetryWaitMs = 3000;

    /**
     * Maximum milliseconds to wait when consuming a snapshot (
     * {@link ConsumerMode#SNAPSHOT}) before timing out.
     */
    private int snapshotTimeoutMs = 10_000;

    /**
     * Maximum milliseconds to wait for a correlated response in
     * {@link ConsumerMode#TRIGGER_THEN_CONSUME} mode.
     */
    private int triggerTimeoutMs = 30_000;

    // -------------------------------------------------------------------------
    // Nested: Connection
    // -------------------------------------------------------------------------

    /**
     * Solace PubSub+ broker connection settings.
     */
    public static class Connection {

        /**
         * Broker host URI (e.g. {@code tcp://localhost:55555} or
         * {@code tcps://broker.example.com:55443}).
         */
        private String host;

        /** Message VPN name (default: {@code default}). */
        private String vpn = "default";

        /** Username used for authentication. */
        private String username;

        /** Password used for authentication. */
        private String password;

        /**
         * Optional client name sent to the broker. If not specified the broker
         * assigns one automatically.
         */
        private String clientName;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public String getVpn() {
            return vpn;
        }

        public void setVpn(String vpn) {
            this.vpn = vpn;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getClientName() {
            return clientName;
        }

        public void setClientName(String clientName) {
            this.clientName = clientName;
        }
    }

    // -------------------------------------------------------------------------
    // Getters and setters
    // -------------------------------------------------------------------------

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public List<SubscriptionDescriptor> getSubscriptions() {
        return subscriptions;
    }

    public void setSubscriptions(List<SubscriptionDescriptor> subscriptions) {
        this.subscriptions = subscriptions;
    }

    public int getReconnectRetries() {
        return reconnectRetries;
    }

    public void setReconnectRetries(int reconnectRetries) {
        this.reconnectRetries = reconnectRetries;
    }

    public int getReconnectRetryWaitMs() {
        return reconnectRetryWaitMs;
    }

    public void setReconnectRetryWaitMs(int reconnectRetryWaitMs) {
        this.reconnectRetryWaitMs = reconnectRetryWaitMs;
    }

    public int getSnapshotTimeoutMs() {
        return snapshotTimeoutMs;
    }

    public void setSnapshotTimeoutMs(int snapshotTimeoutMs) {
        this.snapshotTimeoutMs = snapshotTimeoutMs;
    }

    public int getTriggerTimeoutMs() {
        return triggerTimeoutMs;
    }

    public void setTriggerTimeoutMs(int triggerTimeoutMs) {
        this.triggerTimeoutMs = triggerTimeoutMs;
    }
}
