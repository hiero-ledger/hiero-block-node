// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.remove;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.hiero.block.server.util.PersistTestUtils.PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;
import org.hiero.block.server.Constants;
import org.hiero.block.server.config.BlockNodeContext;
import org.hiero.block.server.persistence.storage.PersistenceStorageConfig;
import org.hiero.block.server.persistence.storage.path.BlockPathResolver;
import org.hiero.block.server.util.TestConfigUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test class for {@link BlockAsLocalFileRemover}.
 */
class BlockAsLocalFileRemoverTest {
    private BlockPathResolver blockPathResolverMock;
    private BlockAsLocalFileRemover toTest;

    @TempDir
    private Path testLiveRootPath;

    @BeforeEach
    void setUp() throws IOException {
        final BlockNodeContext blockNodeContext = TestConfigUtil.getTestBlockNodeContext(
                Map.of(PERSISTENCE_STORAGE_LIVE_ROOT_PATH_KEY, testLiveRootPath.toString()));
        final PersistenceStorageConfig testConfig =
                blockNodeContext.configuration().getConfigData(PersistenceStorageConfig.class);

        final Path testConfigLiveRootPath = testConfig.liveRootPath();
        assertThat(testConfigLiveRootPath).isEqualTo(testLiveRootPath);

        blockPathResolverMock = mock(BlockPathResolver.class);
        toTest = new BlockAsLocalFileRemover(blockPathResolverMock);
    }

    /**
     * This test aims to verify that the
     * {@link BlockAsLocalFileRemover#removeLiveUnverified(long)} correctly
     * deletes a block with the given block number.
     *
     * @param toRemove parameterized, block number
     */
    @ParameterizedTest
    @MethodSource("validBlockNumbers")
    void testSuccessfulBlockDeletion(final long toRemove) throws IOException {
        final String blockPath = String.valueOf(toRemove).concat(Constants.BLOCK_FILE_EXTENSION);
        final String unverifiedBlockPath =
                blockPath.replace(Constants.BLOCK_FILE_EXTENSION, Constants.UNVERIFIED_BLOCK_FILE_EXTENSION);
        final Path unverifiedPath = testLiveRootPath.resolve(unverifiedBlockPath);

        Files.createDirectories(unverifiedPath.getParent());
        Files.createFile(unverifiedPath);

        assertThat(unverifiedPath).exists().isRegularFile().isReadable();

        when(blockPathResolverMock.resolveLiveRawUnverifiedPathToBlock(toRemove))
                .thenReturn(unverifiedPath);
        final boolean actual = toTest.removeLiveUnverified(toRemove);
        assertThat(actual).isTrue();
        assertThat(unverifiedPath).doesNotExist();
    }

    /**
     * This test aims to verify that the
     * {@link BlockAsLocalFileRemover#removeLiveUnverified(long)} returns false
     * when the block file does not exist.
     *
     * @param toRemove parameterized, block number
     */
    @ParameterizedTest
    @MethodSource("validBlockNumbers")
    void testFailedBlockDeletionFileNotExist(final long toRemove) throws IOException {
        final String blockPath = String.valueOf(toRemove).concat(Constants.BLOCK_FILE_EXTENSION);
        final String unverifiedBlockPath =
                blockPath.replace(Constants.BLOCK_FILE_EXTENSION, Constants.UNVERIFIED_BLOCK_FILE_EXTENSION);
        final Path unverifiedPath = testLiveRootPath.resolve(unverifiedBlockPath);

        assertThat(unverifiedPath).doesNotExist();

        when(blockPathResolverMock.resolveLiveRawUnverifiedPathToBlock(toRemove))
                .thenReturn(unverifiedPath);
        final boolean actual = toTest.removeLiveUnverified(toRemove);
        assertThat(actual).isFalse();
        assertThat(unverifiedPath).doesNotExist();
    }

    /**
     * This test aims to verify that the
     * {@link BlockAsLocalFileRemover#removeLiveUnverified(long)} correctly throws an
     * {@link IllegalArgumentException} when an invalid block number is
     * provided. A block number is invalid if it is a strictly negative number.
     *
     * @param toRemove parameterized, block number
     */
    @ParameterizedTest
    @MethodSource("invalidBlockNumbers")
    void testInvalidBlockNumber(final long toRemove) {
        assertThatIllegalArgumentException().isThrownBy(() -> toTest.removeLiveUnverified(toRemove));
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
