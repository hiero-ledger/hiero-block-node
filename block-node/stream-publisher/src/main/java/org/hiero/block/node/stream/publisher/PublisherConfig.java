// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Max;
import com.swirlds.config.api.validation.annotation.Min;
import org.hiero.block.node.base.Loggable;

/**
 * Configuration for a block stream publisher plugin.
 */
@ConfigData("producer")
public record PublisherConfig(
        @Loggable @ConfigProperty(defaultValue = "9_223_372_036_854_775_807") @Min(100_000L) long batchForwardLimit,
        @Loggable @ConfigProperty(defaultValue = "10") @Min(5) @Max(60) int noActivityNotificationIntervalSeconds) {
    // Do not create a constructor just to validate the `@Min`, because that's
    // already done in the Configuration framework initialization.
}
