// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.fixtures.generator;

import static org.hiero.block.simulator.fixtures.blocks.BlockBuilder.createBlocks;

import com.hedera.hapi.block.stream.protoc.Block;
import org.hiero.block.simulator.config.types.GenerationMode;
import org.hiero.block.simulator.generator.BlockStreamManager;

public class TestBlockStreamManager {

    public static BlockStreamManager getTestBlockStreamManager(final int blockCount) {
        return new BlockStreamManager() {
            int blockIndex = 0;

            @Override
            public GenerationMode getGenerationMode() {
                return null;
            }

            @Override
            public Block getNextBlock() {
                if (blockIndex < blockCount) {
                    Block block = createBlocks(blockIndex, blockIndex + 1);
                    blockIndex++;
                    return block;
                }
                return null;
            }

            @Override
            public void resetToBlock(long block) {}
        };
    }
}
