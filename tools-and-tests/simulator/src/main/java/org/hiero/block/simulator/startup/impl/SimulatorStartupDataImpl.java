// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.startup.impl;

import static java.lang.System.Logger.Level.DEBUG;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    private final Path latestAckBlockHashPath;
    private final long startupDataBlockNumber;
    private final byte[] startupDataBlockHash;
    private final Map<Long, byte[]> blockHashStore = new ConcurrentHashMap<>();

    @Inject
    public SimulatorStartupDataImpl(
            @NonNull final SimulatorStartupDataConfig simulatorStartupDataConfig,
            @NonNull final BlockGeneratorConfig blockGeneratorConfig) {
        this.enabled = simulatorStartupDataConfig.enabled();
        this.latestAckBlockNumberPath = simulatorStartupDataConfig.latestAckBlockNumberPath();
        this.latestAckBlockHashPath = simulatorStartupDataConfig.latestAckBlockHashPath();
        long localStartupDataBlockNumber = blockGeneratorConfig.startBlockNumber() - 1L;
        byte[] localStartupDataBlockHash = new byte[StreamingTreeHasher.HASH_LENGTH];
        if (enabled) {
            try {
                final int existsLatestAckBlockNumberFile = Files.exists(latestAckBlockNumberPath) ? 1 : 0;
                final int existsLatestAckBlockHashFile = Files.exists(latestAckBlockHashPath) ? 1 : 0;
                // determine the number of existing startup data files
                final int existingStartupDataFileCount = existsLatestAckBlockNumberFile + existsLatestAckBlockHashFile;
                switch (existingStartupDataFileCount) {
                    case 0 -> {
                        // if no startup data files exist, this means that this
                        // is the initial setup, we only need to create the
                        // startup data files
                        FileUtilities.createFile(latestAckBlockNumberPath);
                        FileUtilities.createFile(latestAckBlockHashPath);
                    }
                    case 1 -> {
                        // if only one file exists, then this is an erroneous
                        // state. We must investigate why this is happening.
                        // Generally we never ever expect to enter here, but
                        // we cannot continue to initialize the simulator
                        throw new IllegalStateException(
                                "Failed to initialize Simulator Startup Data, only one startup data file exists!");
                    }
                    case 2 -> {
                        // entering here means that both files exist, so now we
                        // must attempt to read the startup data from the files.
                        // If both files are empty (e.g. pod restarted before any
                        // blocks were processed), treat as initial state and use
                        // defaults. Otherwise, read the persisted values.
                        final String blockNumberFromFile = Files.readString(latestAckBlockNumberPath);
                        final byte[] previousHashFromFile = Files.readAllBytes(latestAckBlockHashPath);
                        final boolean blockNumberEmpty = StringUtilities.isBlank(blockNumberFromFile);
                        final boolean blockHashEmpty = previousHashFromFile.length == 0;
                        if (blockNumberEmpty && blockHashEmpty) {
                            LOGGER.log(DEBUG, "Both startup data files are empty, treating as initial state");
                        } else if (!blockNumberEmpty
                                && previousHashFromFile.length == StreamingTreeHasher.HASH_LENGTH) {
                            localStartupDataBlockNumber = Long.parseLong(blockNumberFromFile);
                            localStartupDataBlockHash = previousHashFromFile;
                        } else {
                            throw new IllegalStateException(
                                    "Inconsistent Simulator Startup Data: block number empty=%s, hash length=%d"
                                            .formatted(blockNumberEmpty, previousHashFromFile.length));
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
    public void addBlockHash(Long blockNumber, byte[] blockHash) {
        blockHashStore.put(blockNumber, blockHash);
    }

    @Override
    public void updateLatestAckBlockStartupData(final long blockNumber) throws IOException {
        if (enabled) {
            Files.write(latestAckBlockNumberPath, String.valueOf(blockNumber).getBytes());
            Files.write(latestAckBlockHashPath, blockHashStore.get(blockNumber));
            LOGGER.log(DEBUG, "Updated startup data for latest ack block with number: {0}", blockNumber);
        }
    }
}
