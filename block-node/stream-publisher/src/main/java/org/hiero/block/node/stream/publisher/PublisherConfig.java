// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.common.utils.Preconditions;
import org.hiero.block.node.base.Loggable;

/**
 * Configuration for a block stream publisher plugin.
 */
@ConfigData("producer")
public record PublisherConfig(@Loggable @ConfigProperty(defaultValue = "8000") @Min(2000) int timeoutThresholdMillis) {
    public PublisherConfig {
        timeoutThresholdMillis = Math.toIntExact(Preconditions.requireGreaterOrEqual(timeoutThresholdMillis, 2000));
    }
}
