// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.generator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.hedera.hapi.block.stream.protoc.Block;
import com.hedera.hapi.block.stream.protoc.BlockItem;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.hiero.block.simulator.config.data.BlockGeneratorConfig;
import org.hiero.block.simulator.config.types.GenerationMode;
import org.hiero.block.simulator.exception.BlockSimulatorParsingException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BlockAsFileLargeDataSetsTest {

    private static final String rootFolder = "build/resources/test//block-0.0.3-blk/";
    private static int filesInFolder;

    @BeforeAll
    static void setUp() {
        filesInFolder = getFilesInFolder(getAbsoluteFolder(rootFolder));
    }

    @AfterEach
    void tearDown() {}

    @Test
    void getGenerationMode() {
        BlockStreamManager blockStreamManager =
                getBlockAsFileLargeDatasetsBlockStreamManager(getAbsoluteFolder(rootFolder));

        assertEquals(GenerationMode.DIR, blockStreamManager.getGenerationMode());
    }

    @Test
    void getNextBlock() throws IOException, BlockSimulatorParsingException {
        BlockStreamManager blockStreamManager =
                getBlockAsFileLargeDatasetsBlockStreamManager(getAbsoluteFolder(rootFolder));
        for (int i = 0; i < filesInFolder; i++) {
            assertNotNull(blockStreamManager.getNextBlock());
        }

        assertNull(blockStreamManager.getNextBlock());
    }

    @Test
    void getNextBlockInRange() throws IOException, BlockSimulatorParsingException {

        final BlockGeneratorConfig blockGeneratorConfig = BlockGeneratorConfig.builder()
                .generationMode(GenerationMode.DIR)
                .folderRootPath(getAbsoluteFolder(rootFolder))
                .managerImplementation("BlockAsFileBlockStreamManager")
                .paddedLength(36)
                .fileExtension(".blk")
                .startBlockNumber(2)
                .endBlockNumber(4)
                .build();

        final BlockStreamManager blockStreamManager =
                getBlockAsFileLargeDatasetsBlockStreamManager(blockGeneratorConfig);

        // The startBlockNumber and endBlockNumber signal to the manager
        // that it should only return blocks within the specified range.
        // Here, the first 3 should succeed (blocks 2, 3 and 4) but the 4th should
        // return null.
        for (int i = 0; i < 3; i++) {
            assertNotNull(blockStreamManager.getNextBlock());
        }

        assertNull(blockStreamManager.getNextBlock());
    }

    @Test
    void getNextBlockItem() throws IOException, BlockSimulatorParsingException {
        BlockStreamManager blockStreamManager =
                getBlockAsFileLargeDatasetsBlockStreamManager(getAbsoluteFolder(rootFolder));

        while (true) {
            BlockItem blockItem = blockStreamManager.getNextBlockItem();
            if (blockItem == null) {
                break;
            }
            assertNotNull(blockItem);
        }
    }

    @Test
    void getBlockByNumberReturnsBlockIfExists() throws IOException, BlockSimulatorParsingException {
        BlockAsFileLargeDataSets blockStreamManager =
                getBlockAsFileLargeDatasetsBlockStreamManager(getAbsoluteFolder(rootFolder));

        Block block = blockStreamManager.getBlockByNumber(1);
        assertNotNull(block);
        assertNotNull(block.getItemsList()); // optional: sanity check on block content
    }

    @Test
    void getBlockByNumberThrowsIfBlockDoesNotExist() throws IOException {
        BlockAsFileLargeDataSets blockStreamManager =
                getBlockAsFileLargeDatasetsBlockStreamManager(getAbsoluteFolder(rootFolder));

        long nonExistentBlock = 999_999L;
        assertThrows(IOException.class, () -> blockStreamManager.getBlockByNumber(nonExistentBlock));
    }

    @Test
    void getBlockByNumberThrowsForInvalidBlockNumber() throws IOException {
        BlockAsFileLargeDataSets blockStreamManager =
                getBlockAsFileLargeDatasetsBlockStreamManager(getAbsoluteFolder(rootFolder));

        assertThrows(IllegalArgumentException.class, () -> blockStreamManager.getBlockByNumber(0));
    }

    @Test
    void gettingNextBlockItemThrowsParsingException(@TempDir Path tempDir) throws IOException {
        String blockFolderName = "block-0.0.3-blk";
        Path blockDirPath = tempDir.resolve(blockFolderName);
        Files.createDirectories(blockDirPath);

        int nextBlockIndex = 1;
        int paddedLength = 36;
        String fileExtension = ".blk";
        String formatString = "%0" + paddedLength + "d" + fileExtension;

        String currentBlockFileName = String.format(formatString, nextBlockIndex);
        Path currentBlockFilePath = blockDirPath.resolve(currentBlockFileName);

        byte[] invalidData = "invalid block data".getBytes();
        Files.write(currentBlockFilePath, invalidData);

        final BlockGeneratorConfig blockGeneratorConfig = BlockGeneratorConfig.builder()
                .generationMode(GenerationMode.DIR)
                .folderRootPath(blockDirPath.toString())
                .managerImplementation("BlockAsFileBlockStreamManager")
                .paddedLength(36)
                .fileExtension(".blk")
                .build();

        BlockAsFileLargeDataSets blockStreamManager = new BlockAsFileLargeDataSets(blockGeneratorConfig);

        assertThrows(
                com.google.protobuf.InvalidProtocolBufferException.class,
                blockStreamManager::getNextBlock,
                "com.google.protobuf.InvalidProtocolBufferException: Protocol message end-group tag did not match expected tag.");
    }

    private BlockAsFileLargeDataSets getBlockAsFileLargeDatasetsBlockStreamManager(String rootFolder) {

        final BlockGeneratorConfig blockGeneratorConfig = BlockGeneratorConfig.builder()
                .generationMode(GenerationMode.DIR)
                .folderRootPath(rootFolder)
                .managerImplementation("BlockAsFileBlockStreamManager")
                .paddedLength(36)
                .fileExtension(".blk")
                .build();

        return getBlockAsFileLargeDatasetsBlockStreamManager(blockGeneratorConfig);
    }

    private BlockAsFileLargeDataSets getBlockAsFileLargeDatasetsBlockStreamManager(
            BlockGeneratorConfig blockGeneratorConfig) {
        return new BlockAsFileLargeDataSets(blockGeneratorConfig);
    }

    private static String getAbsoluteFolder(String relativePath) {
        return Paths.get(relativePath).toAbsolutePath().toString();
    }

    private static int getFilesInFolder(String absolutePath) {
        File folder = new File(absolutePath);
        File[] blkFiles = folder.listFiles(file -> file.isFile()
                && (file.getName().endsWith(".blk") || file.getName().endsWith(".blk.gz")));
        return blkFiles.length;
    }
}
