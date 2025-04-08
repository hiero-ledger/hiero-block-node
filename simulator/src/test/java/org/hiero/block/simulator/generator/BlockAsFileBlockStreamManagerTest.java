// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator;

import static org.junit.jupiter.api.Assertions.*;

import com.hedera.hapi.block.stream.protoc.Block;
import java.io.IOException;
import java.nio.file.Paths;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.types.GenerationMode;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BlockAsFileBlockStreamManagerTest {

    private final String gzRootFolder = "build/resources/main//block-0.0.3/";
    private BlockStreamManager blockStreamManager;

    private String getAbsoluteFolder(String relativePath) {
        return Paths.get(relativePath).toAbsolutePath().toString();
    }

    @BeforeEach
    void setUp() {
        blockStreamManager = getBlockAsFileBlockStreamManager(getAbsoluteFolder(gzRootFolder));
        blockStreamManager.init();
    }

    @Test
    void getGenerationMode() {
        assertEquals(GenerationMode.DIR, blockStreamManager.getGenerationMode());
    }

    @Test
    void getNextBlock() throws IOException, BlockSimulatorParsingException {
        for (int i = 0; i < 3000; i++) {
            assertNotNull(blockStreamManager.getNextBlock());
        }
    }

    @Test
    void getNextBlockItem() throws IOException, BlockSimulatorParsingException {
        for (int i = 0; i < 35000; i++) {
            assertNotNull(blockStreamManager.getNextBlockItem());
        }
    }

    @Test
    void getLastBlock_ReturnsMostRecentBlock() throws IOException, BlockSimulatorParsingException {
        BlockAsFileBlockStreamManager fileManager = (BlockAsFileBlockStreamManager) blockStreamManager;

        // Move through a few blocks
        fileManager.getNextBlock(); // block 0
        Block block1 = fileManager.getNextBlock(); // block 1

        Block lastBlock = fileManager.getLastBlock();

        assertEquals(block1, lastBlock, "Expected getLastBlock to return the most recently given block");
    }

    @Test
    void getBlockByNumber_ReturnsCorrectBlock() throws IOException, BlockSimulatorParsingException {
        BlockAsFileBlockStreamManager fileManager = (BlockAsFileBlockStreamManager) blockStreamManager;

        Block block0 = fileManager.getBlockByNumber(0);
        assertNotNull(block0, "Expected block at index 0");

        Block block1 = fileManager.getBlockByNumber(1);
        assertNotNull(block1, "Expected block at index 1");

        Block last = fileManager.getBlockByNumber(9999); // Should clamp to last
        Block expectedLast = fileManager.getBlockByNumber(fileManager.blocks.size() - 1);
        assertEquals(expectedLast, last, "Expected out-of-bounds access to return last block");

        Block fromNegative = fileManager.getBlockByNumber(-5); // Should clamp to first
        assertEquals(block0, fromNegative, "Expected negative index to return first block");
    }

    @Test
    void loadBlockBlk() throws IOException, BlockSimulatorParsingException {
        String blkRootFolder = "build/resources/test//block-0.0.3-blk/";
        BlockStreamManager blockStreamManager = getBlockAsFileBlockStreamManager(getAbsoluteFolder(blkRootFolder));
        blockStreamManager.init();

        assertNotNull(blockStreamManager.getNextBlock());
    }

    @Test
    void BlockAsFileBlockStreamManagerInvalidRootPath() {
        assertThrows(
                RuntimeException.class,
                () -> getBlockAsFileBlockStreamManager(
                        getAbsoluteFolder("build/resources/test//BlockAsDirException/1/")));
    }

    private BlockStreamManager getBlockAsFileBlockStreamManager(String rootFolder) {
        BlockGeneratorConfig blockGeneratorConfig = BlockGeneratorConfig.builder()
                .generationMode(GenerationMode.DIR)
                .folderRootPath(rootFolder)
                .managerImplementation("BlockAsFileBlockStreamManager")
                .paddedLength(36)
                .fileExtension(".blk")
                .build();

        BlockStreamManager blockStreamManager = new BlockAsFileBlockStreamManager(blockGeneratorConfig);
        blockStreamManager.init();
        return blockStreamManager;
    }
}
