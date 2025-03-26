// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.startup.impl;

import com.hedera.hapi.block.protoc.PublishStreamResponse;
import com.hedera.hapi.block.protoc.PublishStreamResponse.BlockAcknowledgement;
import com.hedera.hapi.block.protoc.PublishStreamResponseCode;
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
    private final Path latestAckBlockHashPath;
    private final long startupDataBlockNumber;
    private final byte[] startupDataBlockHash;

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
                        // If successful, we can finish initialization, otherwise
                        // we have broken state and cannot continue.
                        final String blockNumberFromFile = Files.readString(latestAckBlockNumberPath);
                        if (!StringUtilities.isBlank(blockNumberFromFile)) {
                            localStartupDataBlockNumber = Long.parseLong(blockNumberFromFile) + 1L;
                        } else {
                            throw new IllegalStateException(
                                    "Failed to initialize latest ack block number from Simulator Startup Data");
                        }
                        final byte[] previousHashFromFile = Files.readAllBytes(latestAckBlockHashPath);
                        if (previousHashFromFile.length == StreamingTreeHasher.HASH_LENGTH) {
                            localStartupDataBlockHash = previousHashFromFile;
                        } else {
                            throw new IllegalStateException(
                                    "Failed to initialize latest ack block hash from Simulator Startup Data");
                        }
                    }
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
    public void updateLatestAckBlockStartupData(@NonNull final PublishStreamResponse publishStreamResponse)
            throws IOException {
        if (enabled) {
            final BlockAcknowledgement ack =
                    publishStreamResponse.getAcknowledgement().getBlockAck();
            final PublishStreamResponseCode responseCode =
                    publishStreamResponse.getStatus().getStatus();
            // @todo(904) we need the correct response code, currently it seems that
            //   the response code is not being set correctly? The if check should
            //   be different and based on the response code, only saving
            if (PublishStreamResponseCode.STREAM_ITEMS_UNKNOWN == responseCode && !ack.getBlockAlreadyExists()) {
                final long blockNumber = ack.getBlockNumber();
                final byte[] blockHash = ack.getBlockRootHash().toByteArray();
                Files.write(
                        latestAckBlockNumberPath, String.valueOf(blockNumber).getBytes());
                Files.write(latestAckBlockHashPath, blockHash);
            }
        }
    }
}
