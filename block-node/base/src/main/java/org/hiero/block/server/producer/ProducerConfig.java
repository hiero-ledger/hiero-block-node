// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.producer;

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
public record ProducerConfig(@Loggable @ConfigProperty(defaultValue = "PRODUCTION") ProducerType type) {
    /**
     * The type of the producer service to use - PRODUCTION or NO_OP.
     */
    public enum ProducerType {
        PRODUCTION,
        NO_OP,
    }
}
