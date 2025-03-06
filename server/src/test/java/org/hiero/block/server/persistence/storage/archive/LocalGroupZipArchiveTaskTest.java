// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIOException;
import static org.hiero.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_ARCHIVE_BATCH_SIZE;
import static org.hiero.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_ARCHIVE_ROOT_PATH_KEY;
import static org.hiero.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_COMPRESSION_TYPE;
import static org.hiero.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY;

import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.UncheckedParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.hiero.block.common.utils.FileUtilities;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig;
import org.hiero.block.server.persistence.storage.path.ArchiveBlockPath;
import org.hiero.block.server.persistence.storage.path.BlockAsLocalFilePathResolver;
import org.hiero.block.server.persistence.storage.path.BlockPathResolver;
import org.hiero.block.server.util.PersistTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

/**
 * Test for {@link LocalGroupZipArchiveTask}.
 */
class LocalGroupZipArchiveTaskTest {
    private static final int ARCHIVE_GROUP_SIZE = 10;
    private static final int THRESHOLD_PASSED_TEN = 10;

    @TempDir
    private Path testTempDir;

    private BlockPathResolver pathResolverSpy;
    private PersistenceStorageConfig persistenceStorageConfig;

    @BeforeEach
    void setUp() throws IOException {

        final Configuration config = ConfigurationBuilder.create()
                .autoDiscoverExtensions()
                .withValue(PERSISTENCE_STORAGE_COMPRESSION_TYPE, "NONE")
                .withValue(PERSISTENCE_STORAGE_ARCHIVE_BATCH_SIZE, String.valueOf(ARCHIVE_GROUP_SIZE))
                .withValue(
                        PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY,
                        testTempDir.resolve("live").toString())
                .withValue(
                        PERSISTENCE_STORAGE_ARCHIVE_ROOT_PATH_KEY,
                        testTempDir.resolve("archive").toString())
                .build();

        persistenceStorageConfig = config.getConfigData(PersistenceStorageConfig.class);
        // using spy for path resolver because we should test with actual logic for path resolution
        // also asserts would be based on the findLive/findArchive methods, which are unit tested themselves
        // in the respective test class
        pathResolverSpy = Mockito.spy(new BlockAsLocalFilePathResolver(persistenceStorageConfig));
    }

    /**
     * This test aims to assert that the archiver correctly counts archived
     * blocks.
     */
    @Test
    void testArchiveBlockCount() throws IOException {
        // write first 10 blocks to live storage before running the archiver
        writeFirstTenBlocks();

        // call the actual archiver
        final LocalGroupZipArchiveTask toTest =
                new LocalGroupZipArchiveTask(THRESHOLD_PASSED_TEN, persistenceStorageConfig, pathResolverSpy);
        final long blocksArchived = toTest.call();

        assertThat(blocksArchived).isEqualTo(ARCHIVE_GROUP_SIZE);
    }

    /**
     * This test aims to assert that the archiver correctly removes blocks from
     * live storage after successful archive.
     */
    @Test
    void testArchiveBlockLocationNotLive() throws IOException {
        // write first 10 blocks to live storage before running the archiver
        writeFirstTenBlocks();

        // call the actual archiver
        final LocalGroupZipArchiveTask toTest =
                new LocalGroupZipArchiveTask(THRESHOLD_PASSED_TEN, persistenceStorageConfig, pathResolverSpy);
        toTest.call();

        // assert that blocks are not in live storage
        for (int blockNumber = 0; blockNumber < THRESHOLD_PASSED_TEN; blockNumber++) {
            assertThat(pathResolverSpy.findLiveBlock(blockNumber)).isNotNull().isEmpty();
        }
    }

    /**
     * This test aims to assert that the archiver correctly stores archived
     * blocks in the archive storage.
     */
    @Test
    void testArchiveBlockLocationInArchive() throws IOException {
        // write first 10 blocks to live storage before running the archiver
        writeFirstTenBlocks();

        // call the actual archiver
        final LocalGroupZipArchiveTask toTest =
                new LocalGroupZipArchiveTask(THRESHOLD_PASSED_TEN, persistenceStorageConfig, pathResolverSpy);
        toTest.call();

        // assert that blocks are in archive storage
        for (int blockNumber = 0; blockNumber < THRESHOLD_PASSED_TEN; blockNumber++) {
            final Optional<ArchiveBlockPath> archivedBlock = pathResolverSpy.findArchivedBlock(blockNumber);
            assertThat(archivedBlock).isNotNull().isPresent();
        }
    }

    /**
     * This test aims to assert that the archiver correctly writes bytes to the
     * archive storage.
     */
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    void testArchiveBlockBytesWritten() throws IOException {
        // write first 10 blocks to live storage before running the archiver
        final List<BlockUnparsed> firstTenBlocks = writeFirstTenBlocks();

        // call the actual archiver
        final LocalGroupZipArchiveTask toTest =
                new LocalGroupZipArchiveTask(THRESHOLD_PASSED_TEN, persistenceStorageConfig, pathResolverSpy);
        toTest.call();

        // assert that what is read as bytes from archive matches what was created and written initially
        for (int blockNumber = 0; blockNumber < THRESHOLD_PASSED_TEN; blockNumber++) {
            final Optional<ArchiveBlockPath> archivedBlock = pathResolverSpy.findArchivedBlock(blockNumber);
            final byte[] actual = readArchived(archivedBlock.get());
            final BlockUnparsed block = firstTenBlocks.get(blockNumber);
            final byte[] expected = BlockUnparsed.PROTOBUF.toBytes(block).toByteArray();
            assertThat(actual).isEqualTo(expected);
        }
    }

    /**
     * This test aims to assert that the archiver returns 0 count when the
     * there are no blocks to archive under root.
     */
    @Test
    void testArchiveBlockCountZero() throws IOException {
        // write something outside the threshold
        final List<List<BlockItemUnparsed>> blocksTenToNineteenAsItems =
                PersistTestUtils.generateBlockItemsUnparsedForBlocksInRangeChunked(10, 20);
        doWriteBlocks(blocksTenToNineteenAsItems, 10, 20);
        // create the root for the archive under live, this is expected to be
        // present for the task to run properly
        Files.createDirectories(pathResolverSpy.resolveRawPathToArchiveParentUnderLive(9));
        // call the actual archiver
        final LocalGroupZipArchiveTask toTest =
                new LocalGroupZipArchiveTask(THRESHOLD_PASSED_TEN, persistenceStorageConfig, pathResolverSpy);
        final long blocksArchived = toTest.call();
        assertThat(blocksArchived).isEqualTo(0);
    }

    @Test
    void testArchiveBlockThrowsExceptionIfZipFileExists() throws IOException {
        // create the zip file that should not exist when starting the archiver
        final Path zipFile = pathResolverSpy.resolveRawPathToArchiveParentUnderArchive(0);
        FileUtilities.createFile(zipFile);
        // call the actual archiver
        final LocalGroupZipArchiveTask toTest =
                new LocalGroupZipArchiveTask(THRESHOLD_PASSED_TEN, persistenceStorageConfig, pathResolverSpy);
        assertThatIOException().isThrownBy(toTest::call);
    }

    private List<BlockUnparsed> writeFirstTenBlocks() throws IOException {
        // generate first 10 blocks, from numbers 0 to 9
        final List<List<BlockItemUnparsed>> firstTenBlocksAsItems =
                PersistTestUtils.generateBlockItemsUnparsedStartFromBlockNumber0Chunked(10);
        // write first 10 blocks to live storage
        return doWriteBlocks(firstTenBlocksAsItems, 0, 10);
    }

    private List<BlockUnparsed> doWriteBlocks(
            final List<List<BlockItemUnparsed>> blocksAsItems, final int startBlockNumber, final int endBlockNumber)
            throws IOException {
        final List<BlockUnparsed> firstTenBlocks = new ArrayList<>();
        for (int blockNumber = startBlockNumber; blockNumber < endBlockNumber; blockNumber++) {
            final long localBlockNumber = blockNumber;
            final List<BlockItemUnparsed> block = blocksAsItems.stream()
                    .filter(b -> {
                        try {
                            final Bytes header =
                                    Objects.requireNonNull(b.getFirst().blockHeader());
                            return BlockHeader.PROTOBUF.parse(header).number() == localBlockNumber;
                        } catch (final ParseException e) {
                            throw new UncheckedParseException(e);
                        }
                    })
                    .findFirst()
                    .get();
            final BlockUnparsed blockUnparsed =
                    BlockUnparsed.newBuilder().blockItems(block).build();
            final Path pathToLive = pathResolverSpy.resolveLiveRawPathToBlock(blockNumber);
            FileUtilities.createFile(pathToLive);
            try (final OutputStream out = Files.newOutputStream(pathToLive)) {
                BlockUnparsed.PROTOBUF.toBytes(blockUnparsed).writeTo(out);
            }
            firstTenBlocks.add(blockUnparsed);
            // assert block is in live storage and not in archive
            assertThat(pathResolverSpy.findLiveBlock(blockNumber)).isNotNull().isPresent();
            assertThat(pathResolverSpy.findArchivedBlock(blockNumber))
                    .isNotNull()
                    .isEmpty();
        }
        return firstTenBlocks;
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
