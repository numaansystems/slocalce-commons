package com.numaansystems.solace.commons.parser;

import org.springframework.context.ApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry that maps topic names (or patterns) to named {@link MessageParser} beans.
 * <p>
 * Topics are matched by exact string first; if no match is found the
 * {@code defaultParser} is returned (if configured).
 */
public class ParserRegistry {

    private static final Logger log = LoggerFactory.getLogger(ParserRegistry.class);

    private final ApplicationContext applicationContext;
    /** topic → parser bean name */
    private final Map<String, String> topicToParserBeanName;
    private final String defaultParserBeanName;

    public ParserRegistry(ApplicationContext applicationContext,
                          Map<String, String> topicToParserBeanName,
                          String defaultParserBeanName) {
        this.applicationContext = applicationContext;
        this.topicToParserBeanName = new ConcurrentHashMap<>(topicToParserBeanName);
        this.defaultParserBeanName = defaultParserBeanName;
    }

    /**
     * Resolve the parser for the given topic.
     *
     * @param topic the source Solace topic
     * @return the parser, or {@code null} if no match found
     */
    public MessageParser<?> resolve(String topic) {
        String beanName = topicToParserBeanName.get(topic);
        if (beanName == null) {
            beanName = defaultParserBeanName;
        }
        if (beanName == null) {
            log.warn("No parser configured for topic '{}' and no default parser defined", topic);
            return null;
        }
        if (!applicationContext.containsBean(beanName)) {
            log.error("Parser bean '{}' not found in application context for topic '{}'", beanName, topic);
            return null;
        }
        return (MessageParser<?>) applicationContext.getBean(beanName, MessageParser.class);
    }
}
