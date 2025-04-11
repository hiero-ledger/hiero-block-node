// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator;

import com.hedera.hapi.block.stream.protoc.Block;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import java.io.IOException;
import org.hiero.block.simulator.config.types.GenerationMode;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;

/**
 * The BlockAsDirBlockStreamManager class implements the BlockStreamManager interface to manage the
 * block stream from a directory.
 */
public class BlockAsDirBlockStreamManager implements BlockStreamManager {

    @Override
    public void init() {
        BlockStreamManager.super.init();
    }

    @Override
    public GenerationMode getGenerationMode() {
        return null;
    }

    @Override
    public BlockItem getNextBlockItem() throws IOException, BlockSimulatorParsingException {
        return null;
    }

    @Override
    public Block getNextBlock() throws IOException, BlockSimulatorParsingException {
        return null;
    }
}
