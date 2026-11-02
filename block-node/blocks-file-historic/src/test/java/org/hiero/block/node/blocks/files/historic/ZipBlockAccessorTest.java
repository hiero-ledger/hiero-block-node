// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.hiero.block.node.base.ParseHelper.standardParse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.hedera.hapi.block.stream.Block;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.ConfigurationBuilder;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor.Format;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Test class for {@link ZipBlockAccessor}.
 */
@DisplayName("ZipBlockAccessor Tests")
class ZipBlockAccessorTest {
    /** The testing in-memory file system. */
    private FileSystem jimfs;
    /** The configuration for the test. */
    private FilesHistoricConfig defaultConfig;
    /** The temporary data directory used for the test. */
    private Path dataTempDir;
    /** The temporary links directory used for the test. */
    private Path linksTempDir;

    /** Set up the test environment before each test. */
    @BeforeEach
    void setup() throws IOException {
        // Initialize the in-memory file system
        jimfs = Jimfs.newFileSystem(
                Configuration.unix()); // Set the default configuration for the test, use jimfs for paths
        dataTempDir = jimfs.getPath("/blocks");
        Files.createDirectories(dataTempDir);
        linksTempDir = dataTempDir.resolve("links");
        Files.createDirectories(linksTempDir);
        defaultConfig =
                createTestConfiguration(dataTempDir, getDefaultConfiguration().compression());
    }

    /**
     * Tear down the test environment after each test.
     */
    @AfterEach
    void tearDown() throws IOException {
        // Close the Jimfs file system
        if (jimfs != null) {
            jimfs.close();
            jimfs = null;
        }
    }

    /**
     * Tests for the {@link ZipBlockAccessor} constructor.
     */
    @Nested
    @DisplayName("Constructor Tests")
    final class ConstructorTests {

        /**
         * This test aims to assert that the constructor of
         * {@link ZipBlockAccessor} throws a {@link NullPointerException} when
         * the input blockPath is null.
         */
        @Test
        @DisplayName("Test constructor throws NullPointerException when blockPath is null")
        @SuppressWarnings("all")
        void testNullBlockPath() {
            assertThatNullPointerException().isThrownBy(() -> new ZipBlockAccessor(null, linksTempDir));
        }
    }

    /**
     * Tests for the {@link ZipBlockAccessor} functionality.
     */
    @Nested
    @DisplayName("Functionality Tests")
    final class FunctionalityTests {
        /**
         * This test aims to verify that the {@link ZipBlockAccessor#blockBytes(Format)}
         * will correctly return a zipped block as bytes. This is the happy path test
         * where the compression type is the same as the compression type used to create
         * the block (zip entry inside the zip file we are trying to read).
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockBytes() returns correctly a persisted block as bytes happy path format")
        @SuppressWarnings("DataFlowIssue")
        void testBlockBytesHappyPathFormat(final CompressionType compressionType) throws IOException {
            // build a test block
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, block.number());
            final Bytes expected = block.bytes();
            // test zipBlockAccessor.blockBytes()
            final ZipBlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, blockPath, expected);
            final Format format = getHappyPathFormat(compressionType);
            // The blockBytes method should return the bytes of the block with the
            // specified format. In order to assert the same bytes, we need to decompress
            // the bytes returned by the blockBytes method and compare them to the expected.
            final Bytes testResult = toTest.blockBytes(format);
            final Bytes actual = Bytes.wrap(compressionType.decompress(testResult.toByteArray()));
            assertThat(actual).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link ZipBlockAccessor#blockBytes(Format)}
         * will correctly return a zipped block as bytes. This is the happy path test
         * where the compression type is the same as the compression type used to create
         * the block (zip entry inside the zip file we are trying to read).
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName(
                "Test blockBytes() returns correctly a persisted block as bytes happy path format - consecutive calls")
        @SuppressWarnings("DataFlowIssue")
        void testBlockBytesHappyPathFormatConsecutiveCalls(final CompressionType compressionType) throws IOException {
            // build a test block
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, block.number());
            final Bytes expected = block.bytes();
            // test zipBlockAccessor.blockBytes()
            final ZipBlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, blockPath, expected);
            final Format format = getHappyPathFormat(compressionType);
            // The blockBytes method should return the bytes of the block with the
            // specified format. In order to assert the same bytes, we need to decompress
            // the bytes returned by the blockBytes method and compare them to the expected.
            final Bytes testResult = toTest.blockBytes(format);
            final Bytes actual = Bytes.wrap(compressionType.decompress(testResult.toByteArray()));
            assertThat(actual).isEqualTo(expected);
            // now we close the accessor
            toTest.close();
            assertThat(blockPath.zipFilePath())
                    .exists()
                    .isReadable()
                    .isWritable()
                    .isNotEmptyFile()
                    .hasExtension("zip");
            // now we create a new accessor to the same block
            final ZipBlockAccessor toTest2 = new ZipBlockAccessor(blockPath, linksTempDir);
            // now we should be able to access the block again
            final Bytes testResult2 = toTest2.blockBytes(format);
            final Bytes actual2 = Bytes.wrap(compressionType.decompress(testResult2.toByteArray()));
            assertThat(actual2).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link ZipBlockAccessor#blockBytes(Format)}
         * will correctly return a zipped block as bytes. This test will always use the
         * {@link Format#ZSTD_PROTOBUF} format to read the block bytes.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockBytes() returns correctly a persisted block as bytes using ZSTD_PROTOBUF format")
        @SuppressWarnings("DataFlowIssue")
        void testBlockBytesZSTDPROTOBUFFormat(final CompressionType compressionType) throws IOException {
            // build a test block
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, block.number());
            final Bytes expected = block.bytes();
            // test zipBlockAccessor.blockBytes()
            final ZipBlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, blockPath, expected);
            // The blockBytes method should return the bytes of the block with the
            // specified format. In order to assert the same bytes, we need to decompress
            // the bytes returned by the blockBytes method and compare them to the expected.
            // For this test, we always use the ZSTD_PROTOBUF format to read the block bytes,
            // no matter the actual compression type used to persist the block. With this format
            // we always expect to be returned the bytes compressed using the ZStandard compression
            // algorithm.
            final Bytes testResult = toTest.blockBytes(Format.ZSTD_PROTOBUF);
            final Bytes actual = Bytes.wrap(CompressionType.ZSTD.decompress(testResult.toByteArray()));
            assertThat(actual).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link ZipBlockAccessor#blockBytes(Format)}
         * will correctly return a zipped block as bytes. This test will always use the
         * {@link Format#ZSTD_PROTOBUF} format to read the block bytes. Here we verify that two
         * consecutive accessors to the same block will return the same block.
         * Closing an accessor does not in any way interfere with the data and
         * the ability to access it.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName(
                "Test blockBytes() returns correctly a persisted block as bytes using ZSTD_PROTOBUF format - consecutive calls")
        @SuppressWarnings("DataFlowIssue")
        void testBlockBytesZSTDPROTOBUFFormatConsecutiveCalls(final CompressionType compressionType)
                throws IOException {
            // build a test block
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, block.number());
            final Bytes expected = block.bytes();
            // test zipBlockAccessor.blockBytes()
            final ZipBlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, blockPath, expected);
            // The blockBytes method should return the bytes of the block with the
            // specified format. In order to assert the same bytes, we need to decompress
            // the bytes returned by the blockBytes method and compare them to the expected.
            // For this test, we always use the ZSTD_PROTOBUF format to read the block bytes,
            // no matter the actual compression type used to persist the block. With this format
            // we always expect to be returned the bytes compressed using the ZStandard compression
            // algorithm.
            assertNotNull(jimfs);
            assertThat(jimfs.isOpen()).isTrue();
            assertThat(toTest.isClosed()).isFalse();
            final Bytes testResult = toTest.blockBytes(Format.ZSTD_PROTOBUF);
            final Bytes actual = Bytes.wrap(CompressionType.ZSTD.decompress(testResult.toByteArray()));
            assertThat(actual).isEqualTo(expected);
            // now we close the accessor
            toTest.close();
            assertThat(blockPath.zipFilePath())
                    .exists()
                    .isReadable()
                    .isWritable()
                    .isNotEmptyFile()
                    .hasExtension("zip");
            // now we create a new accessor to the same block
            final ZipBlockAccessor toTest2 = new ZipBlockAccessor(blockPath, linksTempDir);
            // now we should be able to access the block again
            final Bytes testResult2 = toTest2.blockBytes(Format.ZSTD_PROTOBUF);
            final Bytes actual2 = Bytes.wrap(CompressionType.ZSTD.decompress(testResult2.toByteArray()));
            assertThat(actual2).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link ZipBlockAccessor#blockBytes(Format)}
         * will correctly return a zipped block as bytes. This test will always use the
         * {@link Format#PROTOBUF} format to read the block bytes.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockBytes() returns correctly a persisted block as bytes using PROTOBUF format")
        @SuppressWarnings("DataFlowIssue")
        void testBlockBytesProtobufFormat(final CompressionType compressionType) throws IOException {
            // build a test block
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, block.number());
            final Bytes expected = block.bytes();
            // test zipBlockAccessor.blockBytes()
            final ZipBlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, blockPath, expected);
            // The blockBytes method should return the bytes of the block with the
            // specified format.
            // For this test, we always use the PROTOBUF format to read the block bytes,
            // no matter the actual compression type used to persist the block. With this format
            // we always expect to be returned the bytes to not be compressed.
            final Bytes testResult = toTest.blockBytes(Format.PROTOBUF);
            assertThat(testResult).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link ZipBlockAccessor#blockBytes(Format)}
         * will correctly return a zipped block as bytes. This test will always use the
         * {@link Format#PROTOBUF} format to read the block bytes. Here we verify that two
         * consecutive accessors to the same block will return the same block.
         * Closing an accessor does not in any way interfere with the data and
         * the ability to access it.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName(
                "Test blockBytes() returns correctly a persisted block as bytes using PROTOBUF format - consecutive calls")
        @SuppressWarnings("DataFlowIssue")
        void testBlockBytesProtobufFormatConsecutiveCalls(final CompressionType compressionType) throws IOException {
            // build a test block
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, block.number());
            final Bytes expected = block.bytes();
            // test zipBlockAccessor.blockBytes()
            final ZipBlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, blockPath, expected);
            // The blockBytes method should return the bytes of the block with the
            // specified format.
            // For this test, we always use the PROTOBUF format to read the block bytes,
            // no matter the actual compression type used to persist the block. With this format
            // we always expect to be returned the bytes to not be compressed.
            final Bytes testResult = toTest.blockBytes(Format.PROTOBUF);
            assertThat(testResult).isEqualTo(expected);
            // now we close the accessor
            toTest.close();
            assertThat(blockPath.zipFilePath())
                    .exists()
                    .isReadable()
                    .isWritable()
                    .isNotEmptyFile()
                    .hasExtension("zip");
            // now we create a new accessor to the same block
            final ZipBlockAccessor toTest2 = new ZipBlockAccessor(blockPath, linksTempDir);
            // now we should be able to access the block again
            assertThat(toTest2.blockBytes(Format.PROTOBUF)).isEqualTo(expected);
        }

        /**
         * This test aims to verify that a persisted block can be read with
         * {@link ZipBlockAccessor#blockUnparsed()} and then fully parsed to a {@link Block}.
         * This ensures the round-trip of storing and retrieving zipped blocks works correctly.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test block can be read and parsed from zipped persisted data")
        @SuppressWarnings("DataFlowIssue")
        void testBlockParsedFromUnparsed(final CompressionType compressionType) throws IOException, ParseException {
            // build a test block
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, block.number());
            final Block expected = block.block();
            final Bytes protoBytes = block.bytes();
            // test zipBlockAccessor.blockUnparsed() and parse
            final ZipBlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, blockPath, protoBytes);
            final BlockUnparsed unparsed = toTest.blockUnparsed();
            assertThat(unparsed).isNotNull();
            final Block actual = standardParse(Block.PROTOBUF, BlockUnparsed.PROTOBUF.toBytes(unparsed));
            assertThat(actual).isEqualTo(expected);
        }

        /**
         * This test aims to verify that a persisted zipped block can be read and parsed correctly
         * across subsequent accessor instances. Closing an accessor does not in any way interfere
         * with the data and the ability to access it.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test block can be read and parsed - consecutive calls")
        @SuppressWarnings("DataFlowIssue")
        void testBlockParsedFromUnparsedConsecutiveCalls(final CompressionType compressionType)
                throws IOException, ParseException {
            // build a test block
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, block.number());
            final Block expected = block.block();
            final Bytes protoBytes = block.bytes();
            // test zipBlockAccessor.blockUnparsed() and parse
            final ZipBlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, blockPath, protoBytes);
            final BlockUnparsed unparsed = toTest.blockUnparsed();
            assertThat(unparsed).isNotNull();
            final Block actual = standardParse(Block.PROTOBUF, BlockUnparsed.PROTOBUF.toBytes(unparsed));
            assertThat(actual).isEqualTo(expected);
            // now we close the accessor
            toTest.close();
            assertThat(blockPath.zipFilePath())
                    .exists()
                    .isReadable()
                    .isWritable()
                    .isNotEmptyFile()
                    .hasExtension("zip");
            // now we create a new accessor to the same block
            final ZipBlockAccessor toTest2 = new ZipBlockAccessor(blockPath, linksTempDir);
            // now we should be able to access the block again
            final BlockUnparsed unparsed2 = toTest2.blockUnparsed();
            assertThat(unparsed2).isNotNull();
            final Block actual2 = standardParse(Block.PROTOBUF, BlockUnparsed.PROTOBUF.toBytes(unparsed2));
            assertThat(actual2).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link ZipBlockAccessor#blockUnparsed()}
         * will correctly return a zipped block unparsed.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockUnparsed() returns correctly a persisted block unparsed")
        void testBlockUnparsed(final CompressionType compressionType) throws IOException, ParseException {
            // build a test block
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, block.number());
            final BlockUnparsed expected = block.blockUnparsed();
            final Bytes protoBytes = BlockUnparsed.PROTOBUF.toBytes(expected);
            final ZipBlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, blockPath, protoBytes);
            final BlockUnparsed actual = toTest.blockUnparsed();
            assertThat(actual).isEqualTo(expected);
        }

        /**
         * This test aims to verify that the {@link ZipBlockAccessor#blockUnparsed()}
         * will correctly return a zipped block unparsed. Here we verify that two
         * consecutive accessors to the same block will return the same block.
         * Closing an accessor does not in any way interfere with the data and
         * the ability to access it.
         */
        @ParameterizedTest
        @EnumSource(CompressionType.class)
        @DisplayName("Test blockUnparsed() returns correctly a persisted block unparsed - consecutive calls")
        void testBlockUnparsedConsecutiveCalls(final CompressionType compressionType)
                throws IOException, ParseException {
            // build a test block
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            final FilesHistoricConfig testConfig = createTestConfiguration(dataTempDir, compressionType);
            final BlockPath blockPath = BlockPath.computeBlockPath(testConfig, block.number());
            final BlockUnparsed expected = block.blockUnparsed();
            final Bytes protoBytes = BlockUnparsed.PROTOBUF.toBytes(expected);
            // test zipBlockAccessor.blockUnparsed()
            final ZipBlockAccessor toTest = createBlockAndGetAssociatedAccessor(testConfig, blockPath, protoBytes);
            assertThat(toTest.isClosed()).isFalse();
            assertNotNull(jimfs);
            assertThat(jimfs.isOpen()).isTrue();
            final BlockUnparsed actual = toTest.blockUnparsed();
            assertThat(actual).isEqualTo(expected);
            // now we close the accessor
            toTest.close();
            assertThat(blockPath.zipFilePath())
                    .exists()
                    .isReadable()
                    .isWritable()
                    .isNotEmptyFile()
                    .hasExtension("zip");
            // now we create a new accessor to the same block
            final ZipBlockAccessor toTest2 = new ZipBlockAccessor(blockPath, linksTempDir);
            // now we should be able to access the block again
            assertThat(toTest2.blockUnparsed()).isEqualTo(expected);
        }

        private Format getHappyPathFormat(final CompressionType compressionType) {
            return switch (compressionType) {
                case ZSTD -> Format.ZSTD_PROTOBUF;
                case NONE -> Format.PROTOBUF;
            };
        }
    }

    /**
     * Tests for the temporary hard link each accessor holds on its zip file.
     */
    @Nested
    @DisplayName("Link Tests")
    final class LinkTests {
        private static final int THREAD_COUNT = 16;
        private static final int ACCESSORS_PER_THREAD = 100;
        private static final long WAIT_TIMEOUT_SECONDS = 30L;

        private BlockPath blockPath;
        private Bytes expected;

        @BeforeEach
        void setupBlock() {
            final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
            blockPath = BlockPath.computeBlockPath(defaultConfig, block.number());
            expected = block.bytes();
        }

        @Test
        @DisplayName("should create the accessor link under the links root")
        void testLinkCreatedUnderLinksRoot() throws IOException {
            createBlockAndGetAssociatedAccessor(defaultConfig, blockPath, expected);

            assertThat(listFileNames(linksTempDir)).hasSize(1);
        }

        @Test
        @DisplayName("should not create any file next to the zip archive")
        void testNoLinkNextToZip() throws IOException {
            createBlockAndGetAssociatedAccessor(defaultConfig, blockPath, expected);

            assertThat(listFileNames(blockPath.dirPath()))
                    .containsExactly(blockPath.zipFilePath().getFileName().toString());
        }

        @Test
        @DisplayName("should remove the accessor link on close")
        void testCloseRemovesLink() throws IOException {
            final ZipBlockAccessor toTest = createBlockAndGetAssociatedAccessor(defaultConfig, blockPath, expected);

            toTest.close();

            assertThat(listFileNames(linksTempDir)).isEmpty();
        }

        @Test
        @DisplayName("should keep an accessor readable when another accessor to the same zip closes")
        void testClosingOneAccessorKeepsAnotherReadable() throws IOException, ParseException {
            final ZipBlockAccessor first = createBlockAndGetAssociatedAccessor(defaultConfig, blockPath, expected);
            final ZipBlockAccessor second = new ZipBlockAccessor(blockPath, linksTempDir);

            first.close();

            assertThat(second.blockUnparsed()).isEqualTo(BlockUnparsed.PROTOBUF.parse(expected));
        }

        @Test
        @DisplayName("should open concurrent accessors to the same zip without errors")
        void testConcurrentAccessorsToSameZip() throws Exception {
            createBlockAndGetAssociatedAccessor(defaultConfig, blockPath, expected)
                    .close();
            final Queue<Exception> failures = new ConcurrentLinkedQueue<>();
            final CountDownLatch startGate = new CountDownLatch(1);
            final ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
            try {
                for (int i = 0; i < THREAD_COUNT; i++) {
                    executor.submit(() -> openAndCloseAccessors(startGate, failures));
                }
                startGate.countDown();
                executor.shutdown();
                assertThat(executor.awaitTermination(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        .isTrue();
            } finally {
                executor.shutdownNow();
            }

            assertThat(failures).isEmpty();
        }

        private void openAndCloseAccessors(final CountDownLatch startGate, final Queue<Exception> failures) {
            try {
                startGate.await();
                for (int i = 0; i < ACCESSORS_PER_THREAD; i++) {
                    new ZipBlockAccessor(blockPath, linksTempDir).close();
                }
            } catch (final IOException | InterruptedException e) {
                failures.add(e);
            }
        }

        private List<String> listFileNames(final Path directory) throws IOException {
            try (final Stream<Path> files = Files.list(directory)) {
                return files.map(path -> path.getFileName().toString()).toList();
            }
        }
    }

    private ZipBlockAccessor createBlockAndGetAssociatedAccessor(
            final FilesHistoricConfig testConfig, final BlockPath blockPath, Bytes protoBytes) throws IOException {
        // create & assert existing block file path before call
        Files.createDirectories(blockPath.dirPath());
        // it is important the output stream is closed as the compression writes a footer on close
        Files.createFile(blockPath.zipFilePath());
        final byte[] bytesToWrite;
        switch (testConfig.compression()) {
            case NONE -> bytesToWrite = protoBytes.toByteArray();
            case ZSTD -> {
                final byte[] compressedBytes = protoBytes.toByteArray();
                bytesToWrite = CompressionType.ZSTD.compress(compressedBytes);
            }
            default -> throw new IllegalStateException("Unhandled compression type: " + testConfig.compression());
        }
        try (final ZipOutputStream zipOut = new ZipOutputStream(Files.newOutputStream(blockPath.zipFilePath()))) {
            // create a new zip entry
            final ZipEntry zipEntry = new ZipEntry(blockPath.blockFileName());
            zipOut.putNextEntry(zipEntry);
            zipOut.write(bytesToWrite);
            zipOut.closeEntry();
        }
        assertThat(blockPath.zipFilePath())
                .exists()
                .isReadable()
                .isWritable()
                .isNotEmptyFile()
                .hasExtension("zip");
        try (final FileSystem zipFs = FileSystems.newFileSystem(blockPath.zipFilePath())) {
            final Path root = zipFs.getPath("/");
            assertThat(root).isNotNull().exists().isDirectory().isReadable().isNotEmptyDirectory();
            final Path entry = root.resolve((blockPath.blockFileName()));
            assertThat(entry).isNotNull().exists().isRegularFile().isReadable();
            assertThat(Files.exists(entry)).isTrue();
            final byte[] fromZipEntry = Files.readAllBytes(entry);
            assertThat(fromZipEntry).isEqualTo(bytesToWrite);
        }
        return new ZipBlockAccessor(blockPath, linksTempDir);
    }

    private FilesHistoricConfig createTestConfiguration(final Path dataTepDir, final CompressionType compressionType) {
        final FilesHistoricConfig localDefaultConfig = getDefaultConfiguration();
        return new FilesHistoricConfig(
                dataTepDir,
                compressionType,
                localDefaultConfig.powersOfTenPerZipFileContents(),
                localDefaultConfig.blockRetentionThreshold(),
                localDefaultConfig.maxFilesPerDir(),
                localDefaultConfig.stagedBlockNotificationsEnabled());
    }

    private FilesHistoricConfig getDefaultConfiguration() {
        return ConfigurationBuilder.create()
                .withConfigDataType(FilesHistoricConfig.class)
                .build()
                .getConfigData(FilesHistoricConfig.class);
    }
}
