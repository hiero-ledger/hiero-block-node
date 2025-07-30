// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator;

import static org.hiero.block.simulator.fixtures.TestUtils.getAbsoluteFolder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.types.GenerationMode;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BlockAsFileBlockStreamManagerTest {

    private final String gzRootFolder = "build/resources/test/block-0.0.3-blk/";
    private BlockStreamManager blockStreamManager;

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
        // iterates the block items of the blocks in gzRootFolder
        for (int i = 0; i < 72; i++) {
            assertNotNull(blockStreamManager.getNextBlockItem());
        }
    }

    @Test
    void loadBlockBlk() throws IOException, BlockSimulatorParsingException {
        String blkRootFolder = "build/resources/test/block-0.0.3-blk/";
        BlockStreamManager blockStreamManager = getBlockAsFileBlockStreamManager(getAbsoluteFolder(blkRootFolder));
        blockStreamManager.init();

        assertNotNull(blockStreamManager.getNextBlock());
    }

    @Test
    void BlockAsFileBlockStreamManagerInvalidRootPath() {
        assertThrows(
                RuntimeException.class,
                () -> getBlockAsFileBlockStreamManager(
                        getAbsoluteFolder("build/resources/test/BlockAsDirException/1/")));
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
