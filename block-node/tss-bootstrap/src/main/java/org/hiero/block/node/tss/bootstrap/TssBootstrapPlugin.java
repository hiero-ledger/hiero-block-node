// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.tss.bootstrap;

import java.lang.System.Logger.Level;
import java.nio.file.Files;
import org.hiero.block.node.app.config.node.NodeConfig;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;

public class TssBootstrapPlugin implements BlockNodePlugin {
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The configuration for all plugins */
    private NodeConfig nodeConfig;
    /** True once TSS parameters have been persisted (file bootstrap or successful query peer BN). */
    public static boolean tssDataPersisted;

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        // see if the statusDetail TSS Data file exists
        nodeConfig = context.configuration().getConfigData(NodeConfig.class);
        // serialized TssData (ledger ID, WRAPS VK, and Rosters)
        final var tssParametersFile = nodeConfig.tssDataFilePath();
        if (!Files.exists(tssParametersFile)) {
            // TSS Data is not persisted yet.
            // Contact another blocknode server to get its TssData
            // Use the same config as Backfill Plugin
            // How to get it to the statusDetailPlugin?
            LOGGER.log(Level.DEBUG, "No TssData file found, contacting peer BlockNode");
        } else {
            LOGGER.log(Level.DEBUG, "TssData file found, Nothing to do");
        }
    }
}
