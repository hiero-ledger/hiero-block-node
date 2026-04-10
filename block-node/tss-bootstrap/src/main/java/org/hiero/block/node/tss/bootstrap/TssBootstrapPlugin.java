// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.tss.bootstrap;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Objects;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssRoster;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;

/// A block node plugin that tries to get the latest TssData and makes it available to the `BlockNodeApp` and
/// to the `ServerStatusServicePlugin`
///
/// The TssData is retrieved from TssData sources in the following order:
///  - `TssBootstrapConfig` TssData fields (ledgerId, wrapsVerificationKey, etc)
///  - (todo) Peer BlockNodes Queries other peer BlockNodes periodically for TssData
public class TssBootstrapPlugin implements BlockNodePlugin {
    /// The logger for this class
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /// The block node context, for access to core facilities.
    private BlockNodeContext context;
    /// The application state facility, for updating application state.
    private ApplicationStateFacility applicationStateFacility;
    /// The configuration for verification
    @SuppressWarnings("FieldCanBeLocal")
    private TssBootstrapConfig tssBootstrapConfig;

    /// {@inheritDoc}
    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(TssBootstrapConfig.class);
    }

    /// {@inheritDoc}
    @Override
    public void init(
            BlockNodeContext context,
            ServiceBuilder serviceBuilder,
            @NonNull final ApplicationStateFacility applicationStateFacility) {
        this.context = context;
        this.applicationStateFacility = Objects.requireNonNull(applicationStateFacility);
        tssBootstrapConfig = this.context.configuration().getConfigData(TssBootstrapConfig.class);

        // process the config data
        processTssDataConfiguration(tssBootstrapConfig);

        // todo: query peer BNs for their TssData, if there is no config data, or nothing in the context
    }

    /// {@inheritDoc}
    @Override
    public void onContextUpdate(BlockNodeContext context) {
        // save the context update
        this.context = context;
    }

    /// process the `TssBootstrapConfig`
    ///
    /// if the config data is valid, create the TssData from the config data and send it to the
    /// ApplicationStateFacility
    ///
    /// @param tssBootstrapConfig The `TssBootstrapConfig` containing the TssData information
    private void processTssDataConfiguration(TssBootstrapConfig tssBootstrapConfig) {
        // get the TssData from the config
        String ledgerId64 = tssBootstrapConfig.ledgerId();
        String wrapsVerificationKey64 = tssBootstrapConfig.wrapsVerificationKey();
        String schnorrPublicKey64 = tssBootstrapConfig.schnorrPublicKey();
        long nodeId = tssBootstrapConfig.nodeId();
        long weight = tssBootstrapConfig.weight();

        if (ledgerId64 == null
                || ledgerId64.isBlank()
                || wrapsVerificationKey64 == null
                || wrapsVerificationKey64.isBlank()
                || schnorrPublicKey64 == null
                || schnorrPublicKey64.isBlank()) {
            return;
        }
        TssData tssData = buildTssData(ledgerId64, wrapsVerificationKey64, nodeId, weight, schnorrPublicKey64);
        applicationStateFacility.updateTssData(tssData);
    }

    /// build a `TssData` object from individual fields from the `TssBootstrapConfig`
    ///
    /// @param ledgerId64 The ledgerId base64 String
    /// @param wrapsVerificationKey64 The wrapsVerificationKey base64 string
    /// @param nodeId The node id
    /// @param weight The weight
    /// @param schnorrPublicKey64 The schnorrPublicKey base64 string
    /// @return a `TssData` object
    private TssData buildTssData(
            String ledgerId64, String wrapsVerificationKey64, long nodeId, long weight, String schnorrPublicKey64) {
        RosterEntry rosterEntry = RosterEntry.newBuilder()
                .nodeId(nodeId)
                .weight(weight)
                .schnorrPublicKey(Bytes.fromBase64(schnorrPublicKey64))
                .build();
        TssRoster tssRoster = TssRoster.newBuilder().rosterEntries(rosterEntry).build();
        return TssData.newBuilder()
                .ledgerId(Bytes.fromBase64(ledgerId64))
                .wrapsVerificationKey(Bytes.fromBase64(wrapsVerificationKey64))
                .currentRoster(tssRoster)
                .build();
    }
}
