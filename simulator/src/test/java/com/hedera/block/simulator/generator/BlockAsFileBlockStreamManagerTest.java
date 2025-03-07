// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.simulator.generator;

import static org.junit.jupiter.api.Assertions.*;

import com.hedera.block.simulator.config.data.BlockGeneratorConfig;
import com.hedera.block.simulator.config.types.GenerationMode;
import com.hedera.block.simulator.exception.BlockSimulatorParsingException;
import java.io.IOException;
import java.nio.file.Paths;
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
