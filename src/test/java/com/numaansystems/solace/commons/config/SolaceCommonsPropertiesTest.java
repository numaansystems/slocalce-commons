package com.numaansystems.solace.commons.config;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link SolaceCommonsProperties} binds correctly from
 * application properties.
 */
@SpringBootTest(classes = SolaceCommonsPropertiesTest.TestConfig.class)
@TestPropertySource(properties = {
        "solace.commons.enabled=true",
        "solace.commons.connection.host=tcp://broker:55555",
        "solace.commons.connection.vpn=my-vpn",
        "solace.commons.connection.username=user1",
        "solace.commons.connection.password=secret",
        "solace.commons.connection.client-name=my-client",
        "solace.commons.reconnect-retries=5",
        "solace.commons.reconnect-retry-wait-ms=2000",
        "solace.commons.snapshot-timeout-ms=8000",
        "solace.commons.trigger-timeout-ms=20000",
        "solace.commons.subscriptions[0].topic=market/prices/AAPL",
        "solace.commons.subscriptions[0].mode=STREAMING",
        "solace.commons.subscriptions[1].topic=market/snapshot/AAPL",
        "solace.commons.subscriptions[1].mode=SNAPSHOT",
        "solace.commons.subscriptions[1].stop-on-first-snapshot=true",
        "solace.commons.subscriptions[2].topic=market/response/AAPL",
        "solace.commons.subscriptions[2].mode=TRIGGER_THEN_CONSUME",
        "solace.commons.subscriptions[2].trigger-topic=market/request",
        "solace.commons.subscriptions[2].trigger-payload={\"symbol\":\"AAPL\"}"
})
class SolaceCommonsPropertiesTest {

    @EnableConfigurationProperties(SolaceCommonsProperties.class)
    static class TestConfig {}

    @Autowired
    private SolaceCommonsProperties properties;

    @Test
    void enabled_shouldBeTrue() {
        assertThat(properties.isEnabled()).isTrue();
    }

    @Test
    void connection_shouldBindAllFields() {
        SolaceCommonsProperties.Connection conn = properties.getConnection();
        assertThat(conn.getHost()).isEqualTo("tcp://broker:55555");
        assertThat(conn.getVpn()).isEqualTo("my-vpn");
        assertThat(conn.getUsername()).isEqualTo("user1");
        assertThat(conn.getPassword()).isEqualTo("secret");
        assertThat(conn.getClientName()).isEqualTo("my-client");
    }

    @Test
    void timeouts_shouldBindCorrectly() {
        assertThat(properties.getReconnectRetries()).isEqualTo(5);
        assertThat(properties.getReconnectRetryWaitMs()).isEqualTo(2000);
        assertThat(properties.getSnapshotTimeoutMs()).isEqualTo(8000);
        assertThat(properties.getTriggerTimeoutMs()).isEqualTo(20000);
    }

    @Test
    void subscriptions_shouldContainThreeEntries() {
        List<SubscriptionDescriptor> subs = properties.getSubscriptions();
        assertThat(subs).hasSize(3);
    }

    @Test
    void streamingSubscription_shouldBeConfiguredCorrectly() {
        SubscriptionDescriptor streaming = properties.getSubscriptions().get(0);
        assertThat(streaming.getTopic()).isEqualTo("market/prices/AAPL");
        assertThat(streaming.getMode()).isEqualTo(ConsumerMode.STREAMING);
    }

    @Test
    void snapshotSubscription_shouldHaveStopOnFirstSnapshotEnabled() {
        SubscriptionDescriptor snapshot = properties.getSubscriptions().get(1);
        assertThat(snapshot.getTopic()).isEqualTo("market/snapshot/AAPL");
        assertThat(snapshot.getMode()).isEqualTo(ConsumerMode.SNAPSHOT);
        assertThat(snapshot.isStopOnFirstSnapshot()).isTrue();
    }

    @Test
    void triggerThenConsumeSubscription_shouldBindTriggerFields() {
        SubscriptionDescriptor trigger = properties.getSubscriptions().get(2);
        assertThat(trigger.getTopic()).isEqualTo("market/response/AAPL");
        assertThat(trigger.getMode()).isEqualTo(ConsumerMode.TRIGGER_THEN_CONSUME);
        assertThat(trigger.getTriggerTopic()).isEqualTo("market/request");
        assertThat(trigger.getTriggerPayload()).isEqualTo("{\"symbol\":\"AAPL\"}");
    }

    @Test
    void defaults_shouldApplyWhenNotOverridden() {
        SolaceCommonsProperties defaults = new SolaceCommonsProperties();
        assertThat(defaults.isEnabled()).isTrue();
        assertThat(defaults.getConnection().getVpn()).isEqualTo("default");
        assertThat(defaults.getReconnectRetries()).isEqualTo(3);
        assertThat(defaults.getReconnectRetryWaitMs()).isEqualTo(3000);
        assertThat(defaults.getSnapshotTimeoutMs()).isEqualTo(10_000);
        assertThat(defaults.getTriggerTimeoutMs()).isEqualTo(30_000);
        assertThat(defaults.getSubscriptions()).isEmpty();
    }
}
