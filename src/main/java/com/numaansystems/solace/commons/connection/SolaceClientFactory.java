package com.numaansystems.solace.commons.connection;

import com.numaansystems.solace.commons.config.SolaceCommonsProperties;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory responsible for creating configured {@link SolaceSessionWrapper}
 * instances from {@link SolaceCommonsProperties}.
 *
 * <p>Each call to {@link #createSession()} creates an independent JCSMP session.
 * Callers are responsible for connecting and closing the returned wrapper.
 */
public class SolaceClientFactory {

    private static final Logger log = LoggerFactory.getLogger(SolaceClientFactory.class);

    private final SolaceCommonsProperties properties;

    public SolaceClientFactory(SolaceCommonsProperties properties) {
        this.properties = properties;
    }

    /**
     * Create a new, unconnected {@link SolaceSessionWrapper}.
     *
     * <p>Call {@link SolaceSessionWrapper#connect()} to establish the TCP
     * connection before subscribing or publishing.
     *
     * @return a freshly created session wrapper
     * @throws JCSMPException if the JCSMP session cannot be instantiated
     */
    public SolaceSessionWrapper createSession() throws JCSMPException {
        SolaceCommonsProperties.Connection conn = properties.getConnection();
        log.debug("Creating Solace session for host={} vpn={} username={}",
                conn.getHost(), conn.getVpn(), conn.getUsername());

        JCSMPProperties jcsmpProps = new JCSMPProperties();
        jcsmpProps.setProperty(JCSMPProperties.HOST, conn.getHost());
        jcsmpProps.setProperty(JCSMPProperties.VPN_NAME, conn.getVpn());
        jcsmpProps.setProperty(JCSMPProperties.USERNAME, conn.getUsername());
        if (conn.getPassword() != null) {
            jcsmpProps.setProperty(JCSMPProperties.PASSWORD, conn.getPassword());
        }
        if (conn.getClientName() != null && !conn.getClientName().isBlank()) {
            jcsmpProps.setProperty(JCSMPProperties.CLIENT_NAME, conn.getClientName());
        }

        JCSMPChannelProperties channelProps = (JCSMPChannelProperties)
                jcsmpProps.getProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES);
        channelProps.setReconnectRetries(properties.getReconnectRetries());
        channelProps.setReconnectRetryWaitInMillis(properties.getReconnectRetryWaitMs());

        JCSMPSession session = JCSMPFactory.onlyInstance().createSession(jcsmpProps);
        return new SolaceSessionWrapper(session);
    }

    public SolaceCommonsProperties getProperties() {
        return properties;
    }
}
