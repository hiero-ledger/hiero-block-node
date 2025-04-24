// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.archive.s3;

import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed;
import static org.hiero.block.node.archive.s3.S3ArchivePlugin.LATEST_ARCHIVED_BLOCK_FILE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.minio.DownloadObjectArgs;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.plugintest.PluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor.Format;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;

/**
 * Unit tests for the {@link S3ArchivePlugin} class.
 */
@SuppressWarnings("SameParameterValue")
class S3ArchivePluginTest extends PluginTestBase<S3ArchivePlugin> {
    private static final Instant START_TIME =
            ZonedDateTime.of(2025, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant();
    private static final String BUCKET_NAME = "test-bucket";
    private static final int MINIO_ROOT_PORT = 9000;
    private static final String MINIO_ROOT_USER = "minioadmin";
    private static final String MINIO_ROOT_PASSWORD = "minioadmin";
    public static final Duration ONE_DAY = Duration.of(1, ChronoUnit.DAYS);
    private final MinioClient minioClient;

    @SuppressWarnings("resource")
    public S3ArchivePluginTest() throws Exception {
        // Start MinIO container
        GenericContainer<?> minioContainer = new GenericContainer<>("minio/minio:latest")
                .withCommand("server /data")
                .withExposedPorts(MINIO_ROOT_PORT)
                .withEnv("MINIO_ROOT_USER", MINIO_ROOT_USER)
                .withEnv("MINIO_ROOT_PASSWORD", MINIO_ROOT_PASSWORD);
        minioContainer.start();
        // Initialize MinIO client
        String endpoint = "http://" + minioContainer.getHost() + ":" + minioContainer.getMappedPort(MINIO_ROOT_PORT);
        minioClient = MinioClient.builder()
                .endpoint(endpoint)
                .credentials(MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)
                .build();
        // Create a bucket
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET_NAME).build());
        // Initialize the plugin and set any required configuration
        start(
                new S3ArchivePlugin(),
                new SimpleInMemoryHistoricalBlockFacility(),
                Map.of(
                        "archive.blocksPerFile", "10", // make it easy to test
                        "archive.endpointUrl", endpoint,
                        "archive.bucketName", BUCKET_NAME,
                        "archive.accessKey", MINIO_ROOT_USER,
                        "archive.secretKey", MINIO_ROOT_PASSWORD));
    }

    @Test
    @DisplayName("ArchivePlugin should upload a tar file for single batch of blocks")
    void startWithSingleBatch(@TempDir Path tempDir) throws Exception {
        // create 10 sample blocks, this should trigger the plugin to archive them
        sendBlocks(START_TIME, 0, 9);
        // wait for the plugin to finish archiving
        CompletableFuture.allOf(plugin.pendingUploads.toArray(new CompletableFuture[0]))
                .join();
        // read the lastest block file and check its content
        assertEquals("9", getLastArchivedBlockFile());
        // check that the plugin has archived the blocks
        final Set<String> allObjects = getAllObjects();
        allObjects.forEach(obj -> LOGGER.log(System.Logger.Level.INFO, "Object: " + obj));
        assertTrue(
                allObjects.contains("blocks/2025/01/2025-01-01_00-00-00_0000000000000000000-0000000000000000009.tar"));
        // download archive and valuate its content
        Path tarFile = tempDir.resolve("blocks.tar");
        downloadFile("blocks/2025/01/2025-01-01_00-00-00_0000000000000000000-0000000000000000009.tar", tarFile);
        // list the contents of the tar file, calling external "tar" command
        ProcessBuilder processBuilder =
                new ProcessBuilder("tar", "-tf", tarFile.toAbsolutePath().toString());
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IOException("Failed to list tar file contents, exit code: " + exitCode);
        }
        // Read the output as a list of strings and check all the blocks are there
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            List<String> output = reader.lines().toList();
            for (int i = 0; i <= 9; i++) {
                assertEquals("000000000000000000" + i + ".blk.zstd", output.get(i));
            }
        }
        // extract the tar file to a temporary directory
        ProcessBuilder extractProcessBuilder = new ProcessBuilder(
                "tar",
                "-xf",
                tarFile.toAbsolutePath().toString(),
                "-C",
                tempDir.toAbsolutePath().toString());
        extractProcessBuilder.redirectErrorStream(true);
        Process extractProcess = extractProcessBuilder.start();
        int extractExitCode = extractProcess.waitFor();
        if (extractExitCode != 0) {
            throw new IOException("Failed to extract tar file contents, exit code: " + extractExitCode);
        }
        // check that the blocks are there
        for (int i = 0; i <= 9; i++) {
            Path blockFile = tempDir.resolve("000000000000000000" + i + ".blk.zstd");
            assertTrue(Files.exists(blockFile), "Block file should exist: " + blockFile);
            // read the block file and check its content
            assertArrayEquals(
                    blockNodeContext
                            .historicalBlockProvider()
                            .block(i)
                            .blockBytes(Format.ZSTD_PROTOBUF)
                            .toByteArray(),
                    Files.readAllBytes(blockFile),
                    "block file should match the block content");
        }
    }

    @Test
    @DisplayName("ArchivePlugin should upload multiple tar files for multiple batches of blocks")
    void doTenBatches() {
        // create 10 sample blocks, this should trigger the plugin to archive them
        sendBlocks(START_TIME, 0, 99);
        // wait for the plugin to finish archiving
        CompletableFuture.allOf(plugin.pendingUploads.toArray(new CompletableFuture[0]))
                .join();
        // read the lastest block file and check its content
        assertEquals("99", getLastArchivedBlockFile());
        // check that the plugin has archived the blocks
        final Set<String> allObjects = getAllObjects();
        allObjects.forEach(obj -> LOGGER.log(System.Logger.Level.INFO, "Object: " + obj));
        assertTrue(
                allObjects.contains("blocks/2025/01/2025-01-01_00-00-00_0000000000000000000-0000000000000000009.tar"));
        assertTrue(
                allObjects.contains("blocks/2025/01/2025-01-11_00-00-00_0000000000000000010-0000000000000000019.tar"));
        assertTrue(
                allObjects.contains("blocks/2025/01/2025-01-21_00-00-00_0000000000000000020-0000000000000000029.tar"));
        assertTrue(
                allObjects.contains("blocks/2025/01/2025-01-31_00-00-00_0000000000000000030-0000000000000000039.tar"));
        assertTrue(
                allObjects.contains("blocks/2025/02/2025-02-10_00-00-00_0000000000000000040-0000000000000000049.tar"));
        assertTrue(
                allObjects.contains("blocks/2025/02/2025-02-20_00-00-00_0000000000000000050-0000000000000000059.tar"));
        assertTrue(
                allObjects.contains("blocks/2025/03/2025-03-02_00-00-00_0000000000000000060-0000000000000000069.tar"));
        assertTrue(
                allObjects.contains("blocks/2025/03/2025-03-12_00-00-00_0000000000000000070-0000000000000000079.tar"));
        assertTrue(
                allObjects.contains("blocks/2025/03/2025-03-22_00-00-00_0000000000000000080-0000000000000000089.tar"));
        assertTrue(
                allObjects.contains("blocks/2025/04/2025-04-01_00-00-00_0000000000000000090-0000000000000000099.tar"));
    }

    /**
     * Get all the objects in the bucket.
     *
     * @return Set of object names, aka full path
     */
    private Set<String> getAllObjects() {
        try {
            return StreamSupport.stream(
                            minioClient
                                    .listObjects(ListObjectsArgs.builder()
                                            .bucket(BUCKET_NAME)
                                            .recursive(true)
                                            .build())
                                    .spliterator(),
                            false)
                    .map(result -> {
                        try {
                            return result.get().objectName();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toSet());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the last archived block file contents, throws RuntimeException if file is missing.
     *
     * @return the last archived block file
     */
    private String getLastArchivedBlockFile() {
        try {
            // read the lastest block file and check its content
            return new String(minioClient
                    .getObject(GetObjectArgs.builder()
                            .bucket(BUCKET_NAME)
                            .object(LATEST_ARCHIVED_BLOCK_FILE)
                            .build())
                    .readAllBytes());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Download a file from bucket.
     *
     * @param objectName the name of the object to download
     * @param destFile the destination file path
     */
    private void downloadFile(String objectName, Path destFile) {
        try {
            minioClient.downloadObject(DownloadObjectArgs.builder()
                    .bucket(BUCKET_NAME)
                    .object(objectName)
                    .filename(destFile.toAbsolutePath().toString())
                    .build());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Sends blocks to the plugin for archiving.
     *
     * @param startBlockNumber the starting block number
     * @param endBlockNumber the ending block number
     * @return the consensus time for the next block
     */
    @SuppressWarnings("UnusedReturnValue")
    private Instant sendBlocks(Instant firstBlockTime, long startBlockNumber, long endBlockNumber) {
        final BlockItemUnparsed[] blockItems =
                createNumberOfVerySimpleBlocksUnparsed(startBlockNumber, endBlockNumber, firstBlockTime, ONE_DAY);
        // split into blocks
        List<BlockItemUnparsed> blockItemList = new ArrayList<>();
        long blockNumber = startBlockNumber;
        Instant blockTime = firstBlockTime;
        for (BlockItemUnparsed blockItem : blockItems) {
            if (blockItem.hasBlockHeader()) {
                blockItemList = new ArrayList<>();
            }
            blockItemList.add(blockItem);
            if (blockItem.hasBlockProof()) {
                blockMessaging.sendBlockItems(new BlockItems(blockItemList, blockNumber));
                blockNumber++;
                blockTime = blockTime.plus(ONE_DAY);
            }
        }
        return blockTime;
    }
}
