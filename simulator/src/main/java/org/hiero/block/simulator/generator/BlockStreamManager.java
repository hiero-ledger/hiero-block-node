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
     * @throws IOException                    if a I/O error occurs
     * @throws BlockSimulatorParsingException if a parse error occurs
     */
    BlockItem getNextBlockItem() throws IOException, BlockSimulatorParsingException;

    /**
     * Get the next block.
     *
     * @return the next block
     * @throws IOException                    if a I/O error occurs
     * @throws BlockSimulatorParsingException if a parse error occurs
     */
    Block getNextBlock() throws IOException, BlockSimulatorParsingException;

    Block getLastBlock() throws IOException, BlockSimulatorParsingException;

    Block getBlockByNumber(long blockNumber) throws IOException, BlockSimulatorParsingException;
}
