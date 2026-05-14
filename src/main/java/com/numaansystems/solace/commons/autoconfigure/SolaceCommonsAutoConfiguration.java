package com.numaansystems.solace.commons.autoconfigure;

import com.numaansystems.solace.commons.config.SolaceCommonsProperties;
import com.numaansystems.solace.commons.connection.SolaceClientFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot auto-configuration for the Solace Commons library.
 *
 * <p>This configuration is active when:
 * <ul>
 *   <li>{@code com.solacesystems.jcsmp.JCSMPSession} is on the classpath, and</li>
 *   <li>{@code solace.commons.enabled} is {@code true} (the default).</li>
 * </ul>
 *
 * <p>Beans can be overridden by declaring them in the application context.
 */
@AutoConfiguration
@ConditionalOnClass(JCSMPSession.class)
@EnableConfigurationProperties(SolaceCommonsProperties.class)
@ConditionalOnProperty(prefix = "solace.commons", name = "enabled", havingValue = "true",
        matchIfMissing = true)
public class SolaceCommonsAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public SolaceClientFactory solaceClientFactory(SolaceCommonsProperties properties) {
        return new SolaceClientFactory(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public PipelineRegistry pipelineRegistry() {
        return new PipelineRegistry();
    }

    @Bean
    @ConditionalOnMissingBean
    public CommonsConsumerManager commonsConsumerManager(
            SolaceCommonsProperties properties,
            SolaceClientFactory clientFactory,
            PipelineRegistry pipelineRegistry) {
        return new CommonsConsumerManager(properties, clientFactory, pipelineRegistry);
    }
}
