// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.consumer;

import com.hedera.block.common.utils.Preconditions;
import com.hedera.block.server.config.logging.Loggable;
import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;

/**
 * Use this configuration across the consumer package.
 *
 * @param timeoutThresholdMillis after this time of inactivity, the consumer will be considered
 *     timed out and will be disconnected
 */
@ConfigData("consumer")
public record ConsumerConfig(
        @Loggable @ConfigProperty(defaultValue = "1500") int timeoutThresholdMillis,
        @Loggable @ConfigProperty(defaultValue = "3") int cueHistoricStreamingPaddingBlocks,
        @Loggable @ConfigProperty(defaultValue = "1000") int maxBlockItemBatchSize) {

    static final int minTimeoutThresholdMillis = 1;
    static final int minMaxBlockItemBatchSize = 1;
    static final int minCueHistoricStreamingPaddingBlocks = 1;

    /**
     * Validate the configuration.
     *
     * @throws IllegalArgumentException if the timeoutThresholdMillis is not positive
     */
    public ConsumerConfig {
        Preconditions.requireGreaterOrEqual(timeoutThresholdMillis, minTimeoutThresholdMillis);
        Preconditions.requireGreaterOrEqual(maxBlockItemBatchSize, minMaxBlockItemBatchSize);
        Preconditions.requireGreaterOrEqual(cueHistoricStreamingPaddingBlocks, minCueHistoricStreamingPaddingBlocks);
    }
}
