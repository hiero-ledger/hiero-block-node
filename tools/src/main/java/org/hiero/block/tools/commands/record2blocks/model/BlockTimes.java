// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.record2blocks.model;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Read the block times from the block_times.bin file.
 */
@SuppressWarnings("unused")
public class BlockTimes {
    /** Mapped buffer on the block_times.bin file. */
    private final LongBuffer blockTimes;

    /**
     * Load and map the block_times.bin file into memory.
     *
     * @param blockTimesFile the path to the block_times.bin file
     */
    public BlockTimes(Path blockTimesFile) {
        try {
            // map file into bytebuffer
            final FileChannel fileChannel = FileChannel.open(blockTimesFile, StandardOpenOption.READ);
            final ByteBuffer blockTimesBytes =
                    fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(blockTimesFile));
            fileChannel.close();
            // wrap the ByteBuffer as a LongBuffer so we can easily read longs
            blockTimes = blockTimesBytes.asLongBuffer();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the block time for the given block number.
     *
     * @param blockNumber the block number
     * @return the block time in milliseconds
     */
    public long getBlockTime(int blockNumber) {
        return blockTimes.get(blockNumber);
    }

    /**
     * Get the maximum block number in the block_times.bin file.
     *
     * @return the maximum block number
     */
    public long getMaxBlockNumber() {
        return blockTimes.capacity() - 1;
    }
}
