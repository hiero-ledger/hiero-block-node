// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.config.state;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import java.nio.file.Path;
import org.hiero.block.node.base.Loggable;

/**
 * Configuration for the Application State.
 *
 * @param appStateDataFilePath path where application state, like TSS data (ledger ID, address book, WRAPS VK),
 *     are persisted across restarts as a serialized {@code TssData}.
 * @param appStateUpdateScanInterval The amount of milliseconds that the {@code ApplicationStateFacility} waits between
 *     checking to see if there are any {@code TssData} updates to process.
 * @param appStateUpdateInitialDelay The amount of milliseconds that the {@code ApplicationStateFacility} waits before
 *     checking for the first time if there are any {@code TssData} updates to process.
 */
@ConfigData("appState")
public record ApplicationStateConfig(
        @Loggable
                @ConfigProperty(defaultValue = "/opt/hiero/block-node/node/app-state-data.bin")
                Path appStateDataFilePath,
        @Loggable @ConfigProperty(defaultValue = "500") @Min(100) long appStateUpdateScanInterval,
        @Loggable @ConfigProperty(defaultValue = "100") @Min(100) int appStateUpdateInitialDelay) {}
