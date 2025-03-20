// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.from;
import static org.hiero.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_COMPRESSION_TYPE;
import static org.hiero.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY;
import static org.mockito.Mockito.spy;

import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig;
import org.hiero.block.server.persistence.storage.compression.Compression;
import org.hiero.block.server.persistence.storage.compression.NoOpCompression;
import org.hiero.block.server.persistence.storage.path.BlockAsLocalFilePathResolver;
import org.hiero.block.server.persistence.storage.path.BlockPathResolver;
import org.hiero.block.server.util.PersistTestUtils;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for the {@link BlockAsLocalFileReader} class.
 */
@SuppressWarnings("FieldCanBeLocal")
class BlockAsLocalFileReaderTest {
    private Compression compressionMock;
    private BlockPathResolver blockPathResolverMock;
    private BlockAsLocalFileReader toTest;

    @TempDir
    private Path testTempDir;

    @BeforeEach
    void setUp() throws IOException {
        final Path testLiveRootPath = testTempDir.resolve("live");
        final Configuration config = ConfigurationBuilder.create()
                .withConfigDataType(PersistenceStorageConfig.class)
                .withValue(PERSISTENCE_STORAGE_COMPRESSION_TYPE, "NONE")
                .withValue(PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY, testLiveRootPath.toString())
                .build();
        final PersistenceStorageConfig persistenceStorageConfig = config.getConfigData(PersistenceStorageConfig.class);
        final Path testConfigLiveRootPath = persistenceStorageConfig.liveRootPath();
        assertThat(testConfigLiveRootPath).isEqualTo(testLiveRootPath);
        compressionMock = spy(NoOpCompression.newInstance());
        blockPathResolverMock = spy(new BlockAsLocalFilePathResolver(persistenceStorageConfig));
        toTest = BlockAsLocalFileReader.of(compressionMock, blockPathResolverMock);
    }

    /**
     * This test aims to verify that the {@link BlockAsLocalFileReader#read(long)} correctly reads a block with a
     * given block number and returns a {@code non-empty} {@link Optional}.
     */
    @ParameterizedTest
    @MethodSource("validBlockNumbers")
    void testSuccessfulBlockRead(final long blockNumber) throws IOException, ParseException {
        final List<BlockItemUnparsed> blockItemUnparsed =
                PersistTestUtils.generateBlockItemsUnparsedForWithBlockNumber(blockNumber);
        final Path written = createAndWriteBlockAsFile(blockNumber, blockItemUnparsed);
        assertThat(written).isNotNull().exists().isReadable().isRegularFile().isNotEmptyFile();
        // writing the test data is successful

        final Optional<BlockUnparsed> actual = toTest.read(blockNumber);
        assertThat(actual)
                .isNotNull()
                .isPresent()
                .get(InstanceOfAssertFactories.type(BlockUnparsed.class))
                .isNotNull()
                .isExactlyInstanceOf(BlockUnparsed.class)
                .returns(blockNumber, from(blockUnparsed -> {
                    try {
                        return BlockHeader.PROTOBUF
                                .parse(Objects.requireNonNull(
                                        blockUnparsed.blockItems().getFirst().blockHeader()))
                                .number();
                    } catch (final ParseException e) {
                        throw new RuntimeException(e);
                    }
                }))
                .extracting(BlockUnparsed::blockItems)
                .asInstanceOf(InstanceOfAssertFactories.LIST)
                .isNotNull()
                .isNotEmpty();
    }

    /**
     * This test aims to verify that the {@link BlockAsLocalFileReader#read(long)} correctly reads a block with a
     * given block number and has the same contents as the block that has been persisted.
     */
    @ParameterizedTest
    @MethodSource("validBlockNumbers")
    void testSuccessfulBlockReadContents(final long blockNumber) throws IOException, ParseException {
        final List<BlockItemUnparsed> blockItemUnparsed =
                PersistTestUtils.generateBlockItemsUnparsedForWithBlockNumber(blockNumber);
        final Path written = createAndWriteBlockAsFile(blockNumber, blockItemUnparsed);
        assertThat(written).isNotNull().exists().isReadable().isRegularFile().isNotEmptyFile();
        // writing the test data is successful

        final Optional<BlockUnparsed> actual = toTest.read(blockNumber);
        assertThat(actual)
                .isNotNull()
                .isPresent()
                .get(InstanceOfAssertFactories.type(BlockUnparsed.class))
                .isNotNull()
                .extracting(BlockUnparsed::blockItems)
                .asInstanceOf(InstanceOfAssertFactories.LIST)
                .isNotNull()
                .isNotEmpty()
                .hasSize(blockItemUnparsed.size())
                .containsExactlyElementsOf(blockItemUnparsed);
    }

    /**
     * This test aims to verify that the
     * {@link BlockAsLocalFileReader#read(long) correctly returns an empty {@link Optional} when no block file is
     * found for the given valid block number.
     */
    @ParameterizedTest
    @MethodSource("validBlockNumbers")
    void testEmptyOptWhenNoBLockFileFound(final long blockNumber) throws IOException, ParseException {
        final Optional<BlockUnparsed> actual = toTest.read(blockNumber);
        assertThat(actual).isNotNull().isEmpty();
    }

    /**
     * This test aims to verify that the {@link BlockAsLocalFileReader#read(long)} correctly throws an
     * {@link IllegalArgumentException} when an invalid block number is provided. A block number is invalid if it
     * is a strictly negative number.
     *
     * @param toRead parameterized, block number
     */
    @ParameterizedTest
    @MethodSource("invalidBlockNumbers")
    void testInvalidBlockNumber(final long toRead) {
        assertThatIllegalArgumentException().isThrownBy(() -> toTest.read(toRead));
    }

    private Path createAndWriteBlockAsFile(final long blockNumber, final List<BlockItemUnparsed> blockItemUnparsed)
            throws IOException {
        final BlockUnparsed block =
                BlockUnparsed.newBuilder().blockItems(blockItemUnparsed).build();
        final Path written = blockPathResolverMock.resolveLiveRawPathToBlock(blockNumber);
        Files.createDirectories(written.getParent());
        Files.write(written, BlockUnparsed.PROTOBUF.toBytes(block).toByteArray());
        return written;
    }

    /**
     * Some valid block numbers.
     *
     * @return a stream of valid block numbers
     */
    private static Stream<Arguments> validBlockNumbers() {
        return Stream.of(
                Arguments.of(0L),
                Arguments.of(1L),
                Arguments.of(2L),
                Arguments.of(10L),
                Arguments.of(100L),
                Arguments.of(1_000L),
                Arguments.of(10_000L),
                Arguments.of(100_000L),
                Arguments.of(1_000_000L),
                Arguments.of(10_000_000L),
                Arguments.of(100_000_000L),
                Arguments.of(1_000_000_000L),
                Arguments.of(10_000_000_000L),
                Arguments.of(100_000_000_000L),
                Arguments.of(1_000_000_000_000L),
                Arguments.of(10_000_000_000_000L),
                Arguments.of(100_000_000_000_000L),
                Arguments.of(1_000_000_000_000_000L),
                Arguments.of(10_000_000_000_000_000L),
                Arguments.of(100_000_000_000_000_000L),
                Arguments.of(1_000_000_000_000_000_000L),
                Arguments.of(Long.MAX_VALUE));
    }

    /**
     * Some invalid block numbers.
     *
     * @return a stream of invalid block numbers
     */
    private static Stream<Arguments> invalidBlockNumbers() {
        return Stream.of(
                Arguments.of(-1L),
                Arguments.of(-2L),
                Arguments.of(-10L),
                Arguments.of(-100L),
                Arguments.of(-1_000L),
                Arguments.of(-10_000L),
                Arguments.of(-100_000L),
                Arguments.of(-1_000_000L),
                Arguments.of(-10_000_000L),
                Arguments.of(-100_000_000L),
                Arguments.of(-1_000_000_000L),
                Arguments.of(-10_000_000_000L),
                Arguments.of(-100_000_000_000L),
                Arguments.of(-1_000_000_000_000L),
                Arguments.of(-10_000_000_000_000L),
                Arguments.of(-100_000_000_000_000L),
                Arguments.of(-1_000_000_000_000_000L),
                Arguments.of(-10_000_000_000_000_000L),
                Arguments.of(-100_000_000_000_000_000L),
                Arguments.of(-1_000_000_000_000_000_000L),
                Arguments.of(Long.MIN_VALUE));
    }
}
