// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.startup.impl;

import static java.lang.System.Logger.Level.DEBUG;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.inject.Inject;
import org.hiero.block.common.hasher.StreamingTreeHasher;
import org.hiero.block.common.utils.FileUtilities;
import org.hiero.block.common.utils.StringUtilities;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.data.SimulatorStartupDataConfig;
import org.hiero.block.simulator.startup.SimulatorStartupData;

public final class SimulatorStartupDataImpl implements SimulatorStartupData {
    private final System.Logger LOGGER = System.getLogger(SimulatorStartupDataImpl.class.getName());
    private final boolean enabled;
    private final Path latestAckBlockNumberPath;
    private final long startupDataBlockNumber;
    private final byte[] startupDataBlockHash;

    @Inject
    public SimulatorStartupDataImpl(
            @NonNull final SimulatorStartupDataConfig simulatorStartupDataConfig,
            @NonNull final BlockGeneratorConfig blockGeneratorConfig) {
        this.enabled = simulatorStartupDataConfig.enabled();
        this.latestAckBlockNumberPath = simulatorStartupDataConfig.latestAckBlockNumberPath();
        long localStartupDataBlockNumber = blockGeneratorConfig.startBlockNumber() - 1L;
        byte[] localStartupDataBlockHash = new byte[StreamingTreeHasher.HASH_LENGTH];
        if (enabled) {
            try {
                final int existsLatestAckBlockNumberFile = Files.exists(latestAckBlockNumberPath) ? 1 : 0;
                // manage presence of data file
                switch (existsLatestAckBlockNumberFile) {
                    case 0 -> {
                        // if no startup data files exist, this means that this
                        // is the initial setup, we only need to create the
                        // startup data files
                        FileUtilities.createFile(latestAckBlockNumberPath);
                    }
                    case 1 -> {
                        // entering here means that both files exist, so now we
                        // must attempt to read the startup data from the files.
                        // If successful, we can finish initialization, otherwise
                        // we have broken state and cannot continue.
                        final String blockNumberFromFile = Files.readString(latestAckBlockNumberPath);
                        if (!StringUtilities.isBlank(blockNumberFromFile)) {
                            localStartupDataBlockNumber = Long.parseLong(blockNumberFromFile);
                        } else {
                            throw new IllegalStateException(
                                    "Failed to initialize latest ack block number from Simulator Startup Data");
                        }
                    }
                    default ->
                        throw new IllegalStateException(
                                "Failed to initialize Simulator Startup Data, invalid number of startup data files!");
                }
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        this.startupDataBlockNumber = localStartupDataBlockNumber;
        this.startupDataBlockHash = localStartupDataBlockHash;
    }

    @Override
    public long getLatestAckBlockNumber() {
        return startupDataBlockNumber;
    }

    @Override
    @NonNull
    public byte[] getLatestAckBlockHash() {
        return startupDataBlockHash;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public void updateLatestAckBlockStartupData(final long blockNumber) throws IOException {
        if (enabled) {
            // @todo(904) we need the correct response code, currently it seems that
            //   the response code is not being set correctly? The if check should
            //   be different and based on the response code, only saving
            Files.write(latestAckBlockNumberPath, String.valueOf(blockNumber).getBytes());
            LOGGER.log(DEBUG, "Updated startup data for latest ack block with number: {0}", blockNumber);
        }
    }
}
