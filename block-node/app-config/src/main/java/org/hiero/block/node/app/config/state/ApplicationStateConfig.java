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
 *     Defaults to {@code /opt/hiero/block-node/application-state/rsa-bootstrap-roster.json}.
 *     Single-book deployments that have not yet migrated to the history file continue to use this path.
 * @param rsaAddressBookHistoryFilePath path to the block-number-keyed RSA address book history file
 *     (JSON-encoded {@code RangedAddressBookHistory}). When present this file takes precedence over
 *     {@code rsaBootstrapFilePath} for historical WRB verification. The file is produced by operator
 *     tooling (T3) that converts the CLI's timestamp-keyed address book history into block-number ranges.
 *     Defaults to {@code /opt/hiero/block-node/application-state/rsa-address-book-history.json}.
 * @param blockRangesFilePath path to the JSON file where the stored block range set is persisted.
 *     The file is written automatically every {@code BLOCK_RANGE_PERSIST_INTERVAL} blocks and on
 *     shutdown, then loaded on startup. Block availability is derived from
 *     {@code HistoricalBlockFacility} at query time and is not persisted here.
 * @param knownPublishersFilePath path to the JSON file (a serialized {@code NetworkData}) describing the
 *     known inbound publishers, loaded on startup and exposed for the {@code /statusz/inbound} endpoint.
 * @param inboundPartnersFilePath path to the JSON file (a serialized {@code NetworkData}) describing the
 *     designated inbound partners, loaded on startup and exposed for the {@code /statusz/inbound} endpoint.
 * @param outboundPartnersFilePath path to the JSON file (a serialized {@code NetworkData}) describing the
 *     designated outbound partners, loaded on startup and exposed for the {@code /statusz/outbound} endpoint.
 * @param updateScanInterval The amount of milliseconds that the {@code ApplicationStateFacility} waits between
 *     checking to see if there are any {@code TssData} updates to process.
 * @param updateInitialDelay The amount of milliseconds before the first scheduled scan. Defaults to {@code 0}
 *     because application state is already processed synchronously before the executor starts.
 */
@ConfigData("app.state")
public record ApplicationStateConfig(
        // spotless:off
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/application-state/tss-bootstrap-roster.json") Path tssBootstrapFilePath,
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/application-state/rsa-bootstrap-roster.json") Path rsaBootstrapFilePath,
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/application-state/rsa-address-book-history.json") Path rsaAddressBookHistoryFilePath,
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/application-state/block-ranges.json") Path blockRangesFilePath,
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/application-state/known-publishers.json") Path knownPublishersFilePath,
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/application-state/inbound-partners.json") Path inboundPartnersFilePath,
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/application-state/outbound-partners.json") Path outboundPartnersFilePath,
        @Loggable @ConfigProperty(defaultValue = "500") @Min(100) long updateScanInterval,
        @Loggable @ConfigProperty(defaultValue = "0") @Min(0) int updateInitialDelay) {
        // spotless:on
}
