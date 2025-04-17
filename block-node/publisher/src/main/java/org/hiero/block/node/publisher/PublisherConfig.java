// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.publisher;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import org.hiero.block.server.config.logging.Loggable;

/**
 * Use this configuration across the producer package
 *
 * @param type use a predefined type string to replace the producer component implementation.
 *     Non-PRODUCTION values should only be used for troubleshooting and development purposes.
 */
@ConfigData("producer")
public record PublisherConfig(
        @Loggable @ConfigProperty(defaultValue = "PRODUCTION") PublisherType type,
        @Loggable @ConfigProperty(defaultValue = "1500") int timeoutThresholdMillis
) {
    /**
     * The type of the publisher service to use - PRODUCTION or NO_OP.
     */
    public enum PublisherType {
        /**
         * Production mode, which is the default. Sends all incoming block items to the block messaging service
         */
        PRODUCTION,
        /**
         * No-op mode. Does not send any block items to the block messaging service. Just updates the metrics
         */
        NO_OP,
    }
}
