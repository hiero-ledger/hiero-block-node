// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.config.state;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import java.nio.file.Path;
import org.hiero.block.node.base.Loggable;

/**
 * Use this configuration for Node-wide configuration.
 * <p>
 * Node-wide configuration includes settings useful to _all_ or nearly all
 * plugins. Examples include the earliest block number managed by this node.
 *
 * @param tssBootstrapFilePath path where application state, like TSS data (ledger ID, address book, WRAPS VK),
 *     are persisted across restarts as a serialized {@code TssData}.
 * @param rsaBootstrapFilePath path to the RSA roster bootstrap file (JSON-encoded {@code NodeAddressBook}).
 *     Configured via {@code app.state.rsaBootstrapFilePath}.
 *     Defaults to {@code /opt/hiero/block-node/node/rsa-bootstrap-roster.json}.
 * @param blockRangesFilePath path to the JSON file where both the stored and available block range sets are
 *     persisted together. The file is written automatically every {@code BLOCK_RANGE_PERSIST_INTERVAL}
 *     blocks and on shutdown, then loaded on startup.
 * @param updateScanInterval The amount of milliseconds that the {@code ApplicationStateFacility} waits between
 *     checking to see if there are any {@code TssData} updates to process.
 */
@ConfigData("app.state")
public record ApplicationStateConfig(
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/node/tss-bootstrap-roster.json")
        Path tssBootstrapFilePath,

        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/node/rsa-bootstrap-roster.json")
        Path rsaBootstrapFilePath,

        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/node/block-ranges.json")
        Path blockRangesFilePath,

        @Loggable @ConfigProperty(defaultValue = "500") @Min(100)
        long updateScanInterval,

        @Loggable @ConfigProperty(defaultValue = "100") @Min(100)
        int updateInitialDelay) {}
