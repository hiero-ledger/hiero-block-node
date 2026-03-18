// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.tss.bootstrap;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import org.hiero.block.api.TssData;
import org.hiero.block.node.app.config.node.NodeConfig;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;

public class TssBootstrapPlugin implements BlockNodePlugin {
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The block node context, for access to core facilities. */
    private BlockNodeContext context;
    /** The configuration for all plugins */
    private NodeConfig nodeConfig;
    /** True once TSS parameters have been persisted (file bootstrap or successful query peer BN). */
    public static boolean tssDataPersisted;

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        // setting context and config
        this.context = context;
        nodeConfig = context.configuration().getConfigData(NodeConfig.class);
        // Bootstrap TSS parameters from persisted file if available. The file contains a
        // serialized TssData (ledger ID, WRAPS VK, and Rosters)
        final var tssParametersFile = nodeConfig.tssDataFilePath();
        if (Files.exists(tssParametersFile)) {
            try {
                Bytes fileBytes = Bytes.wrap(Files.readAllBytes(tssParametersFile));
                TssData tssData = TssData.PROTOBUF.parse(fileBytes);
                tssDataPersisted = true;
                LOGGER.log(INFO, "Loaded TSS data from file: {0}", tssParametersFile);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read TSS data file: " + tssParametersFile, e);
            } catch (ParseException e) {
                throw new IllegalStateException("Failed to parse TSS data file: " + tssParametersFile, e);
            }
        } else {
            // todo: query a peer bn for the TSS info
            // then persist the results
            TssData tssData = new TssData.Builder().build();
            persistTssData(tssData);
        }
    }

    protected void persistTssData(TssData tssData) {
        final var tssParametersFile = nodeConfig.tssDataFilePath();
        try {
            Files.createDirectories(tssParametersFile.getParent());
            Bytes serialized = TssData.PROTOBUF.toBytes(tssData);
            Files.write(tssParametersFile, serialized.toByteArray());
            tssDataPersisted = true;
            LOGGER.log(INFO, "Persisted TSS data to file: {0}", tssParametersFile);
        } catch (IOException e) {
            LOGGER.log(
                    WARNING, "Failed to persist TSS data to {0}: {1}".formatted(tssParametersFile, e.getMessage()), e);
        }
    }
}
