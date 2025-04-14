// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.hedera.hapi.block.stream.protoc.Block;
import java.io.IOException;
import java.nio.file.Paths;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.types.GenerationMode;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;
import org.junit.jupiter.api.Test;

class BlockAsDirBlockStreamManagerTest {

    private final String rootFolder = "build/resources/test//blockAsDirExample/";

    private String getAbsoluteFolder(String relativePath) {
        return Paths.get(relativePath).toAbsolutePath().toString();
    }

    @Test
    void getGenerationMode() {
        BlockStreamManager blockStreamManager = getBlockAsDirBlockStreamManager(getAbsoluteFolder(rootFolder));
        assertEquals(GenerationMode.DIR, blockStreamManager.getGenerationMode());

        assertEquals(GenerationMode.DIR, blockStreamManager.getGenerationMode());
    }

    @Test
    void getNextBlockItem() throws IOException, BlockSimulatorParsingException {
        BlockStreamManager blockStreamManager = getBlockAsDirBlockStreamManager(getAbsoluteFolder(rootFolder));
        blockStreamManager.init();

        for (int i = 0; i < 1000; i++) {
            assertNotNull(blockStreamManager.getNextBlockItem());
        }
    }

    @Test
    void getNextBlock() throws IOException, BlockSimulatorParsingException {
        BlockStreamManager blockStreamManager = getBlockAsDirBlockStreamManager(getAbsoluteFolder(rootFolder));
        blockStreamManager.init();

        for (int i = 0; i < 3000; i++) {
            assertNotNull(blockStreamManager.getNextBlock());
        }
    }

    @Test
    void getLastBlock_ReturnsPreviousBlock() throws IOException, BlockSimulatorParsingException {
        BlockAsDirBlockStreamManager blockStreamManager =
                (BlockAsDirBlockStreamManager) getBlockAsDirBlockStreamManager(getAbsoluteFolder(rootFolder));
        blockStreamManager.init();

        // Call getNextBlock twice
        Block firstBlock = blockStreamManager.getNextBlock(); // index 0
        Block secondBlock = blockStreamManager.getNextBlock(); // index 1

        Block lastBlock = blockStreamManager.getLastBlock();

        // Should return same as secondBlock
        assertEquals(secondBlock, lastBlock, "Expected getLastBlock to return the most recently given block");
    }

    @Test
    void getBlockByNumber_ReturnsCorrectBlockAndHandlesBounds() throws IOException, BlockSimulatorParsingException {
        BlockAsDirBlockStreamManager blockStreamManager =
                (BlockAsDirBlockStreamManager) getBlockAsDirBlockStreamManager(getAbsoluteFolder(rootFolder));
        blockStreamManager.init();

        // Should return first block
        Block block0 = blockStreamManager.getBlockByNumber(0);
        assertNotNull(block0);

        // Should return second block (if it exists)
        Block block1 = blockStreamManager.getBlockByNumber(1);
        assertNotNull(block1);

        // Should return last block if index is out of bounds
        Block blockOutOfBounds = blockStreamManager.getBlockByNumber(9999);
        Block lastBlockInList = blockStreamManager.getBlockByNumber(blockStreamManager.blocks.size() - 1);
        assertEquals(lastBlockInList, blockOutOfBounds, "Expected out-of-bounds access to return last block");

        // Negative number should return first block
        Block negativeIndexBlock = blockStreamManager.getBlockByNumber(-5);
        assertEquals(block0, negativeIndexBlock, "Expected negative index to return first block");
    }

    @Test
    void BlockAsFileBlockStreamManagerInvalidRootPath() {
        assertThrows(
                RuntimeException.class,
                () -> getBlockAsDirBlockStreamManager(getAbsoluteFolder("build/resources/test//BlockAsDirException/")));
    }

    private BlockStreamManager getBlockAsDirBlockStreamManager(String rootFolder) {
        final BlockGeneratorConfig blockGeneratorConfig = new BlockGeneratorConfig(
                GenerationMode.DIR, 1, 10, 1, 10, rootFolder, "BlockAsDirBlockStreamManager", 36, ".blk", 0, 0);
        final BlockStreamManager blockStreamManager = new BlockAsDirBlockStreamManager(blockGeneratorConfig);
        blockStreamManager.init();
        return blockStreamManager;
    }
}
