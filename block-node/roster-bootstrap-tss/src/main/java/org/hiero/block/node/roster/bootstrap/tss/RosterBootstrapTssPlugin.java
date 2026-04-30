// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.tss;

import static java.lang.System.Logger.Level.WARNING;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import org.hiero.block.api.TssData;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;

/// A block node plugin that tries to get the latest TssData and makes it available to the `BlockNodeApp` and
/// to the `ServerStatusServicePlugin`
///
/// The TssData is retrieved from TssData sources in the following order:
///  - `RosterBootstrapTssConfig` TssData fields (ledgerId, wrapsVerificationKey, etc)
///  - (todo) Peer BlockNodes Queries other peer BlockNodes periodically for TssData
public class RosterBootstrapTssPlugin implements BlockNodePlugin {
    /// The application state facility, for updating application state.
    private ApplicationStateFacility applicationStateFacility;

    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    /// {@inheritDoc}
    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(RosterBootstrapTssConfig.class);
    }

    /// {@inheritDoc}
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        this.applicationStateFacility = Objects.requireNonNull(context.applicationStateFacility());
        RosterBootstrapTssConfig rosterBootstrapTssConfig =
                context.configuration().getConfigData(RosterBootstrapTssConfig.class);

        // process the config data
        processTssDataConfiguration(rosterBootstrapTssConfig);

        // todo: query peer BNs for their TssData, if there is no config data, or nothing in the context
    }

    /// process the `RosterBootstrapTssConfig`
    ///
    /// if the config data is valid, create the TssData from the config data and send it to the
    /// ApplicationStateFacility
    ///
    /// @param rosterBootstrapTssConfig The `RosterBootstrapTssConfig` containing the TssData information
    private void processTssDataConfiguration(RosterBootstrapTssConfig rosterBootstrapTssConfig) {
        Path tssDataJsonPath = Path.of(rosterBootstrapTssConfig.tssDataJsonPath());
        try {
            TssData tssData = TssData.JSON.parse(Bytes.wrap(Files.readAllBytes(tssDataJsonPath)));
            applicationStateFacility.updateTssData(tssData);
        } catch (ParseException | IOException e) {
            final String parseFailedMsg = "Failed to parse TssData configuratiion from path: [%s], %s"
                    .formatted(rosterBootstrapTssConfig.tssDataJsonPath(), e.getMessage());
            LOGGER.log(WARNING, parseFailedMsg, e);
        }
    }
}
