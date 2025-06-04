// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.data;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import org.hiero.block.simulator.config.logging.Loggable;

@ConfigData("consumer")
public record ConsumerConfig(
        @Loggable @ConfigProperty(defaultValue = "-1") long startBlockNumber,
        @Loggable @ConfigProperty(defaultValue = "-1") long endBlockNumber,
        @Loggable @ConfigProperty(defaultValue = "false") boolean verification,
        @Loggable @ConfigProperty(defaultValue = "false") boolean slowDown,
        @Loggable @ConfigProperty(defaultValue = "2") long slowDownMilliseconds,
        @Loggable @ConfigProperty(defaultValue = "0") long slowDownAfterBlock,
        @Loggable @ConfigProperty(defaultValue = "10-30") String slowDownForBlockRange) {}
