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
 * @param rsaBootstrapFilePath path to the RSA roster bootstrap file (JSON-encoded {@code NodeAddressBook}) or
 *     the block-number-keyed RSA address book history file (JSON-encoded {@code RangedAddressBookHistory}).
 *     Configured via {@code app.state.rsaBootstrapFilePath}.
 *     Defaults to {@code /opt/hiero/block-node/application-state/rsa-bootstrap-roster.json}.
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
 * @param publisherInboundCategory the name of the category this block node should use for publisher
 *     connections; as defined by the provisioner for this block node.
 * @param enableNodeStateChangeTracking A flag to enable or disable tracking state changes for
 *     `Node` entries to fill in the current Node Store (a.k.a. address book data) for determining
 *     valid publishers and publisher endpoints.
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
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/application-state/block-ranges.json") Path blockRangesFilePath,
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/application-state/known-publishers.json") Path knownPublishersFilePath,
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/application-state/inbound-partners.json") Path inboundPartnersFilePath,
        @Loggable @ConfigProperty(defaultValue = "/opt/hiero/block-node/application-state/outbound-partners.json") Path outboundPartnersFilePath,
        @Loggable @ConfigProperty(defaultValue = "publisher") String publisherInboundCategory,
        @Loggable @ConfigProperty(defaultValue = "false") boolean enableNodeStateChangeTracking,
        @Loggable @ConfigProperty(defaultValue = "500") @Min(100) long updateScanInterval,
        @Loggable @ConfigProperty(defaultValue = "0") @Min(0) int updateInitialDelay) {
        // spotless:on
}
