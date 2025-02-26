// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.archive;

import static com.hedera.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_ARCHIVE_BATCH_SIZE;
import static com.hedera.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_ARCHIVE_ROOT_PATH_KEY;
import static com.hedera.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_COMPRESSION_TYPE;
import static com.hedera.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.block.common.utils.FileUtilities;
import com.hedera.block.server.config.TestConfigBuilder;
import com.hedera.block.server.persistence.storage.PersistenceStorageConfig;
import com.hedera.block.server.persistence.storage.path.ArchiveBlockPath;
import com.hedera.block.server.persistence.storage.path.BlockAsLocalFilePathResolver;
import com.hedera.block.server.persistence.storage.path.BlockPathResolver;
import com.hedera.block.server.util.PersistTestUtils;
import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.swirlds.config.api.Configuration;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * Test for {@link LocalGroupZipArchiveTask}.
 */
class LocalGroupZipArchiveTaskTest {
    private static final int ARCHIVE_GROUP_SIZE = 10;

    @TempDir
    private Path testTempDir;

    private BlockPathResolver pathResolverSpy;
    private PersistenceStorageConfig persistenceStorageConfig;

    @BeforeEach
    void setUp() throws IOException {
        final Configuration config = new TestConfigBuilder(PersistenceStorageConfig.class)
                .withValue(PERSISTENCE_STORAGE_COMPRESSION_TYPE, "NONE")
                .withValue(PERSISTENCE_STORAGE_ARCHIVE_BATCH_SIZE, String.valueOf(ARCHIVE_GROUP_SIZE))
                .withValue(PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY, testTempDir.resolve("live"))
                .withValue(PERSISTENCE_STORAGE_ARCHIVE_ROOT_PATH_KEY, testTempDir.resolve("archive"))
                .getOrCreateConfig();
        persistenceStorageConfig = config.getConfigData(PersistenceStorageConfig.class);
        // using spy for path resolver because we should test with actual logic for path resolution
        // also asserts would be based on the findLive/findArchive methods, which are unit tested themselves
        // in the respective test class
        pathResolverSpy = Mockito.spy(new BlockAsLocalFilePathResolver(persistenceStorageConfig));
    }

    /**
     * This test aims to assert that the archiver correctly archives blocks that
     * have passed the threshold. The archived blocks need to be taken from the
     * live storage and moved to the archive storage.
     */
    @Test
    void testArchiveBlock() throws IOException {
        final int thresholdPassed = 10;
        // generate first 10 blocks, from numbers 0 to 9
        final List<List<BlockItemUnparsed>> firstTenBlocksAsItems =
                PersistTestUtils.generateBlockItemsUnparsedStartFromBlockNumber0Chunked(thresholdPassed);

        // write first 10 blocks to live storage before running the archiver
        final List<BlockUnparsed> firstTenBlocks = new ArrayList<>();
        for (int blockNumber = 0; blockNumber < firstTenBlocksAsItems.size(); blockNumber++) {
            final List<BlockItemUnparsed> block = firstTenBlocksAsItems.get(blockNumber);
            final BlockUnparsed blockUnparsed =
                    BlockUnparsed.newBuilder().blockItems(block).build();
            final Path pathToLive = pathResolverSpy.resolveLiveRawPathToBlock(blockNumber);
            FileUtilities.createFile(pathToLive);
            try (final OutputStream out = Files.newOutputStream(pathToLive)) {
                BlockUnparsed.PROTOBUF.toBytes(blockUnparsed).writeTo(out);
            }
            firstTenBlocks.add(blockUnparsed);
        }

        // assert that blocks are in live storage and not in archive
        for (int blockNumber = 0; blockNumber < thresholdPassed; blockNumber++) {
            assertThat(pathResolverSpy.findLiveBlock(blockNumber)).isNotNull().isPresent();
            assertThat(pathResolverSpy.findArchivedBlock(blockNumber))
                    .isNotNull()
                    .isEmpty();
        }

        // call the actual archiver
        final LocalGroupZipArchiveTask toTest =
                new LocalGroupZipArchiveTask(thresholdPassed, persistenceStorageConfig, pathResolverSpy);
        final long blocksArchived = toTest.call();

        assertThat(blocksArchived).isEqualTo(ARCHIVE_GROUP_SIZE);

        // assert that blocks are in archive storage and in live
        final List<ArchiveBlockPath> archiveBlockPaths = new ArrayList<>();
        for (int blockNumber = 0; blockNumber < thresholdPassed; blockNumber++) {
            assertThat(pathResolverSpy.findLiveBlock(blockNumber)).isNotNull().isEmpty();
            final Optional<ArchiveBlockPath> archivedBlock = pathResolverSpy.findArchivedBlock(blockNumber);
            assertThat(archivedBlock).isNotNull().isPresent();
            archiveBlockPaths.add(archivedBlock.get());
        }

        // assert that what is read as bytes from archive matches what was created and written initially
        for (int blockNumber = 0; blockNumber < archiveBlockPaths.size(); blockNumber++) {
            final byte[] actual = readArchived(archiveBlockPaths.get(blockNumber));
            final BlockUnparsed block = firstTenBlocks.get(blockNumber);
            final byte[] expected = BlockUnparsed.PROTOBUF.toBytes(block).toByteArray();
            assertThat(actual).isEqualTo(expected);
        }
    }

    private byte[] readArchived(final ArchiveBlockPath archiveBlockPath) throws IOException {
        final Path zipFilePath = archiveBlockPath.dirPath().resolve(archiveBlockPath.zipFileName());
        final byte[] value;
        try (final ZipFile zipFile = new ZipFile(zipFilePath.toFile())) {
            final ZipEntry entry = zipFile.getEntry(archiveBlockPath.zipEntryName());
            final InputStream in = zipFile.getInputStream(entry);
            value = in.readAllBytes();
        }
        return value;
    }
}
