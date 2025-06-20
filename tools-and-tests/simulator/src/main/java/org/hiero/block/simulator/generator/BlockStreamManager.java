// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator;

import com.hedera.hapi.block.stream.protoc.Block;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import java.io.IOException;
import org.hiero.block.simulator.config.types.GenerationMode;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;

/** The block stream manager interface. */
public interface BlockStreamManager {

    /**
     * Initialize the block stream manager and load blocks into memory.
     */
    default void init() {}

    /**
     * Get the generation mode.
     *
     * @return the generation mode
     */
    GenerationMode getGenerationMode();

    /**
     * Get the next block item.
     *
     * @return the next block item
     * @throws IOException if a I/O error occurs
     * @throws BlockSimulatorParsingException if a parse error occurs
     */
    BlockItem getNextBlockItem() throws IOException, BlockSimulatorParsingException;

    /**
     * Get the next block.
     *
     * @return the next block
     * @throws IOException if a I/O error occurs
     * @throws BlockSimulatorParsingException if a parse error occurs
     */
    Block getNextBlock() throws IOException, BlockSimulatorParsingException;

    /**
     * Reset the block stream manager to a specific block.
     *
     * @param block the block number to reset to
     */
    void resetToBlock(final long block);
}
