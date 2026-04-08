// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.tss.bootstrap;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
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
///  - `tss-parameters.bin` TssData persisted by the plugin
///  - Peer BlockNodes (future) Queries other peer BlockNodes periodically for TssData
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
        tssBootstrapConfig = context.configuration().getConfigData(TssBootstrapConfig.class);

        final var tssParametersFile = tssBootstrapConfig.tssParametersFilePath();
        // environment config takes precedence
        // try processing the config data first.
        // process the file second
        if (!processedTssDataConfiguration(tssBootstrapConfig) && Files.exists(tssParametersFile)) {
            try {
                Bytes fileBytes = Bytes.wrap(Files.readAllBytes(tssParametersFile));
                TssData tssData = TssData.PROTOBUF.parse(fileBytes);
                applicationStateFacility.updateTssData(tssData);
                LOGGER.log(INFO, "Loaded TSS parameters from file: {0}", tssParametersFile);
                // todo: notify app
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read TSS parameters file: " + tssParametersFile, e);
            } catch (ParseException e) {
                throw new IllegalStateException("Failed to parse TSS parameters file: " + tssParametersFile, e);
            }
        }
        // todo: query peer BNs for their TssData
    }

    /// {@inheritDoc}
    @Override
    public void onContextUpdate(BlockNodeContext context) {
        // save the context update
        this.context = context;
        // write out the new TssData when we receive the Context update
        // This could be from this plugin or the verification plugin or any other plugin that
        // wants to update the TssData
        final var tssParametersFile = tssBootstrapConfig.tssParametersFilePath();
        try {
            Files.createDirectories(tssParametersFile.getParent());
            Bytes serialized = TssData.PROTOBUF.toBytes(this.context.tssData());
            Files.write(tssParametersFile, serialized.toByteArray());
            LOGGER.log(INFO, "Persisted TssData to file: {0}", tssParametersFile);
        } catch (IOException e) {
            LOGGER.log(
                    WARNING, "Failed to persist TssData to {0}: {1}".formatted(tssParametersFile, e.getMessage()), e);
        }
    }

    /// process the `TssBootstrapConfig`
    ///
    /// if the config data is valid, use that data to create the TssData and send it to the ApplicationStateFacility
    ///
    /// @param tssBootstrapConfig The `TssBootstrapConfig` containing the TssData information
    /// @return A boolean indication `true` if the TssData was processed, `false` if it was not
    private boolean processedTssDataConfiguration(TssBootstrapConfig tssBootstrapConfig) {
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
            return false;
        }
        ;

        TssData tssData = buildTssData(ledgerId64, wrapsVerificationKey64, nodeId, weight, schnorrPublicKey64);
        applicationStateFacility.updateTssData(tssData);
        return true;
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
