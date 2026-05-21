// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import static java.time.ZoneOffset.UTC;
import static org.hiero.block.tools.records.RecordFileDates.FIRST_BLOCK_TIME_INSTANT;
import static org.hiero.block.tools.records.RecordFileDates.blockTimeLongToInstant;
import static org.hiero.block.tools.records.RecordFileDates.instantToBlockTimeLong;

import java.io.IOException;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import org.hiero.block.tools.config.NetworkConfig;
import org.hiero.block.tools.metadata.MetadataFiles;

/**
 * Read the block times from the block_times.bin file using memory mapping.
 */
@SuppressWarnings("unused")
public class BlockTimeReader implements AutoCloseable {
    /** Mapped buffer on the block_times.bin file. */
    private LongBuffer mappedLongBuffer;

    /** Genesis instant for this network - used to convert relative block times to absolute times. */
    private final Instant genesisInstant;

    /** Whether to use the RecordFileDates helper methods (mainnet) or direct calculations (custom network). */
    private final boolean useMainnetHelpers;

    /**
     * Load and map the default block_times.bin file into memory.
     * Uses mainnet genesis time.
     *
     * @throws IOException if an I/O error occurs
     */
    public BlockTimeReader() throws IOException {
        this(MetadataFiles.BLOCK_TIMES_FILE, FIRST_BLOCK_TIME_INSTANT);
    }

    /**
     * Load and map the block_times.bin file into memory.
     * Uses mainnet genesis time.
     *
     * @param blockTimesFile the path to the block_times.bin file
     * @throws IOException if an I/O error occurs
     */
    public BlockTimeReader(Path blockTimesFile) throws IOException {
        this(blockTimesFile, FIRST_BLOCK_TIME_INSTANT);
    }

    /**
     * Load and map the block_times.bin file into memory with a custom genesis time.
     *
     * @param blockTimesFile the path to the block_times.bin file
     * @param genesisInstant the genesis instant for this network
     * @throws IOException if an I/O error occurs
     */
    public BlockTimeReader(Path blockTimesFile, Instant genesisInstant) throws IOException {
        this.genesisInstant = genesisInstant;
        this.useMainnetHelpers = FIRST_BLOCK_TIME_INSTANT.equals(genesisInstant);
        try (FileChannel channel = FileChannel.open(blockTimesFile, StandardOpenOption.READ)) {
            this.mappedLongBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size())
                    .asLongBuffer();
        }
    }

    /**
     * Load and map the block_times.bin file into memory using the genesis time from the given network config.
     *
     * @param blockTimesFile the path to the block_times.bin file
     * @param networkConfig the network configuration containing the genesis timestamp
     * @throws IOException if an I/O error occurs
     */
    public BlockTimeReader(Path blockTimesFile, NetworkConfig networkConfig) throws IOException {
        this(blockTimesFile, parseGenesisFromConfig(networkConfig));
    }

    /**
     * Create a BlockTimeReader using the default block_times.bin file and the current network configuration.
     * This is the recommended constructor for network-aware tools.
     *
     * @return a BlockTimeReader configured for the current network
     * @throws IOException if an I/O error occurs
     */
    public static BlockTimeReader forCurrentNetwork() throws IOException {
        return new BlockTimeReader(MetadataFiles.BLOCK_TIMES_FILE, NetworkConfig.current());
    }

    /**
     * Create a BlockTimeReader for the given network.
     *
     * @param blockTimesFile the path to the block_times.bin file
     * @return a BlockTimeReader configured for the current network
     * @throws IOException if an I/O error occurs
     */
    public static BlockTimeReader forCurrentNetwork(Path blockTimesFile) throws IOException {
        return new BlockTimeReader(blockTimesFile, NetworkConfig.current());
    }

    /**
     * Parse the genesis instant from a network config's timestamp string.
     * Converts underscores to colons for ISO-8601 parsing.
     *
     * @param config the network configuration
     * @return the genesis instant
     */
    private static Instant parseGenesisFromConfig(NetworkConfig config) {
        return Instant.parse(config.genesisTimestamp().replace('_', ':'));
    }

    /**
     * Get the block time for the given block number.
     *
     * @param blockNumber the block number
     * @return the block time in nanoseconds
     */
    public long getBlockTime(long blockNumber) {
        return mappedLongBuffer.get((int) blockNumber);
    }

    /**
     * Get the block time for the given block number.
     *
     * @param blockNumber the block number
     * @return the block time as Instant
     */
    public Instant getBlockInstant(long blockNumber) {
        int index = (int) blockNumber;
        int limit = mappedLongBuffer.limit();
        if (index < 0 || index >= limit) {
            throw new IndexOutOfBoundsException("Block " + blockNumber + " is out of bounds. "
                    + "block_times.bin only contains data for blocks 0-" + (limit - 1) + ". "
                    + "Try running UpdateBlockData to fetch latest block times from mirror node.");
        }
        long relativeNanos = mappedLongBuffer.get(index);
        // Use RecordFileDates helper for mainnet, direct calculation for custom networks
        return useMainnetHelpers ? blockTimeLongToInstant(relativeNanos) : genesisInstant.plusNanos(relativeNanos);
    }

    /**
     * Get the block time for the given block number.
     *
     * @param blockNumber the block number
     * @return the block time as Instant
     * @throws IndexOutOfBoundsException if blockNumber is beyond the bounds of the block_times.bin file
     */
    public LocalDateTime getBlockLocalDateTime(long blockNumber) {
        int index = (int) blockNumber;
        int limit = mappedLongBuffer.limit();
        if (index < 0 || index >= limit) {
            throw new IndexOutOfBoundsException("Block " + blockNumber + " is out of bounds. "
                    + "block_times.bin only contains data for blocks 0-" + (limit - 1) + ". "
                    + "Try running UpdateBlockData to fetch latest block times from mirror node, "
                    + "then reload the BlockTimeReader.");
        }
        long relativeNanos = mappedLongBuffer.get(index);
        // Use RecordFileDates helper for mainnet, direct calculation for custom networks
        Instant instant =
                useMainnetHelpers ? blockTimeLongToInstant(relativeNanos) : genesisInstant.plusNanos(relativeNanos);
        return instant.atZone(UTC).toLocalDateTime();
    }

    /**
     * Get the maximum block number in the block_times.bin file.
     *
     * @return the maximum block number
     */
    public long getMaxBlockNumber() {
        return mappedLongBuffer.limit() - 1;
    }

    /**
     * Get the index of the nearest block at or after the target time.
     *
     * @param targetTime the target time in nanoseconds
     * @return the index of the nearest block at or after the target time
     */
    public long getNearestBlockAfterTime(long targetTime) {
        int low = 0;
        int high = mappedLongBuffer.limit() - 1;
        while (low < high) {
            // calculate mid point with consideration for potential overflow
            int mid = low + (high - low) / 2;
            long midVal = mappedLongBuffer.get(mid);
            if (midVal < targetTime) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

    /**
     * Get the index of the nearest block at or after the target time.
     *
     * @param targetTime the target time as LocalDateTime
     * @return the index of the nearest block at or after the target time
     */
    public long getNearestBlockAfterTime(LocalDateTime targetTime) {
        Instant targetInstant = targetTime.toInstant(UTC);
        // Use RecordFileDates helper for mainnet, direct calculation for custom networks
        long relativeNanos = useMainnetHelpers
                ? instantToBlockTimeLong(targetInstant)
                : Duration.between(genesisInstant, targetInstant).toNanos();
        return getNearestBlockAfterTime(relativeNanos);
    }

    @Override
    public void close() {
        mappedLongBuffer = null;
    }
}
