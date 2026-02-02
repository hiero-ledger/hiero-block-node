// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import static java.time.ZoneOffset.UTC;
import static org.hiero.block.tools.records.RecordFileDates.blockTimeLongToInstant;
import static org.hiero.block.tools.records.RecordFileDates.instantToBlockTimeLong;

import java.io.IOException;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDateTime;
import org.hiero.block.tools.metadata.MetadataFiles;

/**
 * Read the block times from the block_times.bin file using memory mapping.
 */
@SuppressWarnings("unused")
public class BlockTimeReader implements AutoCloseable {
    /** Mapped buffer on the block_times.bin file. */
    private LongBuffer mappedLongBuffer;

    /**
     * Load and map the default block_times.bin file into memory.
     *
     * @throws IOException if an I/O error occurs
     */
    public BlockTimeReader() throws IOException {
        this(MetadataFiles.BLOCK_TIMES_FILE);
    }

    /**
     * Load and map the block_times.bin file into memory.
     *
     * @param blockTimesFile the path to the block_times.bin file
     * @throws IOException if an I/O error occurs
     */
    public BlockTimeReader(Path blockTimesFile) throws IOException {
        try (FileChannel channel = FileChannel.open(blockTimesFile, StandardOpenOption.READ)) {
            this.mappedLongBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size())
                    .asLongBuffer();
        }
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
        return blockTimeLongToInstant(mappedLongBuffer.get((int) blockNumber));
    }

    /**
     * Get the block time for the given block number.
     *
     * @param blockNumber the block number
     * @return the block time as Instant
     */
    public LocalDateTime getBlockLocalDateTime(long blockNumber) {
        return blockTimeLongToInstant(mappedLongBuffer.get((int) blockNumber))
                .atZone(UTC)
                .toLocalDateTime();
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
        return getNearestBlockAfterTime(instantToBlockTimeLong(targetTime.toInstant(UTC)));
    }

    @Override
    public void close() {
        mappedLongBuffer = null;
    }
}
