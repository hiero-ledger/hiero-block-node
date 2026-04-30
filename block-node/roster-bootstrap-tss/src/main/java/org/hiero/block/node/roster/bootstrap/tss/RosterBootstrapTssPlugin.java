// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.roster.bootstrap.tss;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.hiero.block.api.TssData;
import org.hiero.block.node.roster.boostrap.tss.BlockNodeSource;
import org.hiero.block.node.roster.boostrap.tss.BlockNodeSourceConfig;
import org.hiero.block.node.roster.bootstrap.tss.client.BlockNodeClient;
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
    /** The logger for this class. */
    private final System.Logger LOGGER = System.getLogger(getClass().getName());

    /// The block node context, for access to core facilities.
    private volatile BlockNodeContext blockNodeContext;
    /// The application state facility, for updating application state.
    private ApplicationStateFacility applicationStateFacility;

    private boolean hasBNSourcesPath = false;
    /** The ScheduledExecutorService used by the RosterBootstrapPlugin to query peer BNs for TssData */
    private ScheduledExecutorService queryPeerExecutor;
    /** The config information for the RosterBootstrapTssConfig*/
    private RosterBootstrapTssConfig rosterBootstrapTssConfig;
    /**
     * Map of BlockNodeSourceConfig to BlockNodeClient instances.
     * This allows us to reuse clients for the same node configuration.
     * Package-private for testing.
     */
    final ConcurrentHashMap<BlockNodeSourceConfig, BlockNodeClient> nodeClientMap = new ConcurrentHashMap<>();

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
        rosterBootstrapTssConfig = context.configuration().getConfigData(RosterBootstrapTssConfig.class);
        this.blockNodeContext = context;

        // process the config data
        processTssDataConfiguration(rosterBootstrapTssConfig);

        // Validate block node sources configuration
        final String sourcesPath = rosterBootstrapTssConfig.blockNodeSourcesPath();
        if (sourcesPath == null || sourcesPath.isBlank()) {
            LOGGER.log(DEBUG, "No block node sources path configured, TssBootstrapPlugin will not query any peers");
            return;
        }

        Path blockNodeSourcesPath = Path.of(rosterBootstrapTssConfig.blockNodeSourcesPath());
        if (!Files.isRegularFile(blockNodeSourcesPath)) {
            final String blockNodeSourcesPathNotFoundMsg =
                    "Block node sources path does not exist or is not a regular file: [{0}], TssBootstrapPlugin will not query any peers";
            LOGGER.log(WARNING, blockNodeSourcesPathNotFoundMsg, rosterBootstrapTssConfig.blockNodeSourcesPath());
            return;
        }

        try {
            BlockNodeSource blockNodeSources =
                    BlockNodeSource.JSON.parse(Bytes.wrap(Files.readAllBytes(blockNodeSourcesPath)));
            // Let the logs know what we loaded.
            for (BlockNodeSourceConfig node : blockNodeSources.nodes()) {
                LOGGER.log(INFO, "Loaded backfill source node: {0}", node);
            }
            hasBNSourcesPath = true;
        } catch (ParseException | IOException e) {
            final String parseFailedMsg =
                    "Failed to parse block node sources from path: [%s], TssBootstrapPlugin will not query any peers: %s"
                            .formatted(rosterBootstrapTssConfig.blockNodeSourcesPath(), e.getMessage());
            LOGGER.log(WARNING, parseFailedMsg, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        if (queryPeerExecutor != null) queryPeerExecutor.shutdown();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() {
        if (!hasBNSourcesPath) {
            return;
        }
        // save the reference of this volatile object.
        BlockNodeContext context = blockNodeContext;
        LOGGER.log(INFO, "ApplicationStateFacility start called");
        Thread.UncaughtExceptionHandler handler =
                (thread, e) -> LOGGER.log(INFO, "Uncaught exception in thread: " + thread.getName(), e);

        // Create thread executors via threadPoolManager.
        queryPeerExecutor =
                context.threadPoolManager().createVirtualThreadScheduledExecutor(1, "ApplicationStateScanner", handler);

        // Schedule periodic gap detection task using autonomous executor
        queryPeerExecutor.scheduleAtFixedRate(
                this::queryPeerTssData,
                rosterBootstrapTssConfig.queryPeerInitialDelay(),
                rosterBootstrapTssConfig.queryPeerInterval(),
                TimeUnit.MILLISECONDS);
    }

    /// {@inheritDoc}
    /// This method is called on a separate thread. Make sure this.context is marked as `volatile`
    @Override
    public void onContextUpdate(BlockNodeContext context) {
        // save the context update
        this.blockNodeContext = context;
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

    /// queries peer BlockNodes for their TssData
    private void queryPeerTssData() {
        // todo: add in the code to query the peers
    }
}
