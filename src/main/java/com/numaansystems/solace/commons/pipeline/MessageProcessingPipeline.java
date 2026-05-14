package com.numaansystems.solace.commons.pipeline;

import com.solacesystems.jcsmp.BytesXMLMessage;

/**
 * Orchestrates the processing chain for a single incoming Solace message:
 * <ol>
 *   <li>Decode raw bytes/text to a typed payload</li>
 *   <li>Apply zero or more filters (drop on first rejection)</li>
 *   <li>Apply zero or more enrichers in order</li>
 *   <li>Invoke the final handler</li>
 * </ol>
 *
 * <p>Use {@link PipelineBuilder} to construct instances.
 *
 * @param <T> the payload type
 */
public interface MessageProcessingPipeline<T> {

    /**
     * Process an incoming raw Solace message through the full pipeline.
     *
     * @param rawMessage the raw JCSMP message received from the broker
     */
    void process(BytesXMLMessage rawMessage);
}
