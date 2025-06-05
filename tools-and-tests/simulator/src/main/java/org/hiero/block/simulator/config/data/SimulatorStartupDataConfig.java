// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.data;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import java.nio.file.Path;
import java.util.Objects;
import org.hiero.block.simulator.config.logging.Loggable;

/**
 * Config record for the startup data functionality.
 *
 * @param enabled whether the startup data functionality is enabled
 * @param latestAckBlockNumberPath path to the file containing the latest
 * acknowledged block number
 * @param latestAckBlockHashPath path to the file containing the latest
 */
@ConfigData("simulator.startup.data")
public record SimulatorStartupDataConfig(
        @Loggable @ConfigProperty(defaultValue = "false") boolean enabled,
        @Loggable @ConfigProperty(defaultValue = "/opt/simulator/data/latestAckBlockNumber")
                Path latestAckBlockNumberPath,
        @Loggable @ConfigProperty(defaultValue = "/opt/simulator/data/latestAckBlockHash")
                Path latestAckBlockHashPath) {
    public SimulatorStartupDataConfig {
        Objects.requireNonNull(latestAckBlockNumberPath);
        Objects.requireNonNull(latestAckBlockHashPath);
    }
}
