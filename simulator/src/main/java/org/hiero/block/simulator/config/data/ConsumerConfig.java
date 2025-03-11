// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.data;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;

@ConfigData("consumer")
public record ConsumerConfig(
        @ConfigProperty(defaultValue = "0") long startBlockNumber,
        @ConfigProperty(defaultValue = "0") long endBlockNumber) {}
