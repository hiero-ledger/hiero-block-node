// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.simulator.config.data;

import static org.junit.jupiter.api.Assertions.*;

import com.hedera.block.simulator.config.types.GenerationMode;
import com.hedera.block.simulator.config.types.SimulatorMode;
import com.hedera.block.simulator.config.types.StreamingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

class BlockStreamConfigTest {

    private String getAbsoluteFolder(String relativePath) {
        return Paths.get(relativePath).toAbsolutePath().toString();
    }

    private BlockStreamConfig.Builder getBlockStreamConfigBuilder() {
        final StreamingMode streamingMode = StreamingMode.CONSTANT_RATE;
        final int delayBetweenBlockItems = 1_500_000;
        final int maxBlockItemsToStream = 10_000;
        final int millisPerBlock = 1000;
        final int blockItemsBatchSize = 1000;

        return BlockStreamConfig.builder()
                .delayBetweenBlockItems(delayBetweenBlockItems)
                .maxBlockItemsToStream(maxBlockItemsToStream)
                .streamingMode(streamingMode)
                .millisecondsPerBlock(millisPerBlock)
                .blockItemsBatchSize(blockItemsBatchSize);
    }

    private BlockGeneratorConfig.Builder getBlockGeneratorConfigBuilder() {
        String folderRootPath = "src/main/resources/block-0.0.3/";
        GenerationMode generationMode = GenerationMode.DIR;

        String blockStreamManagerImplementation = "BlockAsFileBlockStreamManager";
        int paddedLength = 36;
        String fileExtension = ".blk";
        return BlockGeneratorConfig.builder()
                .generationMode(generationMode)
                .folderRootPath(folderRootPath)
                .managerImplementation(blockStreamManagerImplementation)
                .paddedLength(paddedLength)
                .fileExtension(fileExtension);
    }

    @Test
    void testStreamConfigBuilder() {
        BlockStreamConfig config = getBlockStreamConfigBuilder().build();
        // assert
        assertEquals(StreamingMode.CONSTANT_RATE, config.streamingMode());
    }

    @Test
    void testSimulatorPublishClientMode() {
        BlockStreamConfig config = getBlockStreamConfigBuilder()
                .simulatorMode(SimulatorMode.PUBLISHER_CLIENT)
                .build();

        assertEquals(SimulatorMode.PUBLISHER_CLIENT, config.simulatorMode());
    }

    @Test
    void testSimulatorPublishServerMode() {
        BlockStreamConfig config = getBlockStreamConfigBuilder()
                .simulatorMode(SimulatorMode.PUBLISHER_SERVER)
                .build();

        assertEquals(SimulatorMode.PUBLISHER_SERVER, config.simulatorMode());
    }

    @Test
    void testLastKnownStatusesCapacity() {
        final int capacity = 20;
        BlockStreamConfig config = getBlockStreamConfigBuilder()
                .lastKnownStatusesCapacity(capacity)
                .build();

        assertEquals(capacity, config.lastKnownStatusesCapacity());
    }

    @Test
    void testValidAbsolutePath() {
        // Setup valid folder path and generation mode
        String gzRootFolder = "src/main/resources/block-0.0.3/";
        String folderRootPath = getAbsoluteFolder(gzRootFolder);
        GenerationMode generationMode = GenerationMode.DIR;

        // Assume the folder exists
        Path path = Paths.get(folderRootPath);
        assertTrue(Files.exists(path), "The folder must exist for this test.");

        // No exception should be thrown
        BlockGeneratorConfig config = getBlockGeneratorConfigBuilder()
                .folderRootPath(folderRootPath)
                .generationMode(generationMode)
                .build();

        assertEquals(folderRootPath, config.folderRootPath());
        assertEquals(GenerationMode.DIR, config.generationMode());
    }

    @Test
    void testEmptyFolderRootPath() {
        // Setup empty folder root path and generation mode
        String folderRootPath = "";
        GenerationMode generationMode = GenerationMode.DIR;
        BlockGeneratorConfig.Builder builder =
                getBlockGeneratorConfigBuilder().folderRootPath(folderRootPath).generationMode(generationMode);

        BlockGeneratorConfig config = builder.build();

        // Verify that the path is set to the default
        Path expectedPath = Paths.get("src/main/resources/block-0.0.3/").toAbsolutePath();
        assertEquals(expectedPath.toString(), config.folderRootPath());
        assertEquals(GenerationMode.DIR, config.generationMode());
    }

    @Test
    void testRelativeFolderPathThrowsException() {
        // Setup a relative folder path and generation mode
        String relativeFolderPath = "relative/path/to/blocks";
        GenerationMode generationMode = GenerationMode.DIR;

        // An exception should be thrown because the path is not absolute
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> getBlockGeneratorConfigBuilder()
                        .folderRootPath(relativeFolderPath)
                        .generationMode(generationMode)
                        .build());

        // Verify the exception message
        assertEquals(relativeFolderPath + " Root path must be absolute", exception.getMessage());
    }

    @Test
    void testNonExistentFolderThrowsException() {
        // Setup a non-existent folder path and generation mode
        String folderRootPath = "/non/existent/path/to/blocks";
        GenerationMode generationMode = GenerationMode.DIR;

        // Mock Files.notExists to return true
        Path path = Paths.get(folderRootPath);
        assertTrue(Files.notExists(path), "The folder must not exist for this test.");

        // An exception should be thrown because the folder does not exist
        IllegalArgumentException exception =
                assertThrows(IllegalArgumentException.class, () -> getBlockGeneratorConfigBuilder()
                        .folderRootPath(folderRootPath)
                        .generationMode(generationMode)
                        .build());

        // Verify the exception message
        assertEquals("Folder does not exist: " + path, exception.getMessage());
    }

    @Test
    void testGenerationModeNonDirDoesNotCheckFolderExistence() {
        // Setup a non-existent folder path but with a generation mode that is not DIR
        String folderRootPath = "/non/existent/path/to/blocks";
        GenerationMode generationMode = GenerationMode.CRAFT;

        // No exception should be thrown because generation mode is not DIR
        BlockGeneratorConfig config = getBlockGeneratorConfigBuilder()
                .folderRootPath(folderRootPath)
                .generationMode(generationMode)
                .build();

        // Verify that the configuration was created successfully
        assertEquals(folderRootPath, config.folderRootPath());
        assertEquals(GenerationMode.CRAFT, config.generationMode());
    }
}
