// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.data;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import org.hiero.block.simulator.config.logging.Loggable;

/**
 * Defines the configuration data for the consumer.
 *
 * @param startBlockNumber the block number from which to start consuming
 * @param endBlockNumber   the block number at which to stop consuming
 */
@ConfigData("consumer")
public record ConsumerConfig(
        @Loggable @ConfigProperty(defaultValue = "-1") long startBlockNumber,
        @Loggable @ConfigProperty(defaultValue = "-1") long endBlockNumber) {}
