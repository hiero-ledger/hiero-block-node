// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.file.Path;
import java.util.stream.Stream;
import org.hiero.block.server.Constants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for the {@link NoOpBlockPathResolver} class.
 */
class NoOpBlockPathResolverTest {
    private NoOpBlockPathResolver toTest;

    @BeforeEach
    void setUp() {
        toTest = new NoOpBlockPathResolver();
    }

    /**
     * This test aims to verify that the
     * {@link NoOpBlockPathResolver#resolveLiveRawPathToBlock(long)} correctly resolves
     * the path to a block by a given number. The no-op resolver does nothing,
     * always returns a path resolved under '/tmp' based on the blockNumber and
     * has no preconditions check. E.g. for blockNumber 0, the resolved path is
     * '/tmp/hashgraph/blocknode/data/0.tmp.blk'.
     *
     * @param toResolve parameterized, block number
     */
    @ParameterizedTest
    @MethodSource({"validBlockNumbers", "invalidBlockNumbers"})
    void testSuccessfulLiveRawPathResolution(final long toResolve, final Path expected) {
        final Path actual = toTest.resolveLiveRawPathToBlock(toResolve);
        assertThat(actual).isNotNull().isAbsolute().isEqualByComparingTo(expected);
    }

    /**
     * This test aims to verify that the
     * {@link NoOpBlockPathResolver#resolveLiveRawUnverifiedPathToBlock (long)}
     * correctly resolves the path to a block by a given number. The no-op
     * resolver does nothing, always returns a path resolved under '/tmp' based
     * on the blockNumber and has no preconditions check. E.g. for blockNumber
     * 0, the resolved path is '/tmp/hashgraph/blocknode/data/0.tmp.blk'.
     * Essentially, acts the same as
     * {@link NoOpBlockPathResolver#resolveLiveRawPathToBlock(long)}.
     *
     * @param toResolve parameterized, block number
     */
    @ParameterizedTest
    @MethodSource({"validBlockNumbers", "invalidBlockNumbers"})
    void testSuccessfulLiveRawUnverifiedPathResolution(final long toResolve, final Path expected) {
        final Path actual = toTest.resolveLiveRawUnverifiedPathToBlock(toResolve);
        assertThat(actual).isNotNull().isAbsolute().isEqualByComparingTo(expected);
    }

    /**
     * This test aims to verify that the
     * {@link NoOpBlockPathResolver#resolveRawPathToArchiveParentUnderLive(long)}
     * correctly resolves the path to an archive root under live, based on group
     * size as to where a given block by number would reside. The no-op resolver
     * does nothing, always returns a path resolved under '/tmp' based on the
     * blockNumber and has no preconditions check. E.g. for blockNumber 0, the
     * resolved path is '/tmp/hashgraph/blocknode/data/0.tmp.blk'.
     *
     */
    @ParameterizedTest
    @MethodSource({"validBlockNumbers", "invalidBlockNumbers"})
    void testSuccessfulResolveParentToArchiveUnderLive(final long toResolve, final Path expected) {
        final Path actual = toTest.resolveRawPathToArchiveParentUnderLive(toResolve);
        assertThat(actual).isNotNull().isAbsolute().isEqualByComparingTo(expected);
    }

    /**
     * This test aims to verify that the
     * {@link NoOpBlockPathResolver#resolveRawPathToArchiveParentUnderArchive(long)}
     * correctly resolves the path to an archive root under archive, based on
     * group size as to where a given block by number would reside. The no-op
     * resolver does nothing, always returns a path resolved under '/tmp' based
     * on the blockNumber and has no preconditions check. E.g. for blockNumber
     * 0, the resolved path is '/tmp/hashgraph/blocknode/data/0.tmp.blk'.
     *
     */
    @ParameterizedTest
    @MethodSource({"validBlockNumbers", "invalidBlockNumbers"})
    void testSuccessfulResolveParentToArchivedBlocks(final long toResolve, final Path expected) {
        final Path actual = toTest.resolveRawPathToArchiveParentUnderArchive(toResolve);
        assertThat(actual).isNotNull().isAbsolute().isEqualByComparingTo(expected);
    }

    /**
     * This test aims to verify that the
     * {@link NoOpBlockPathResolver#findLiveBlock(long)}  always returns an empty
     * optional.
     *
     * @param toResolve parameterized, block number
     */
    @ParameterizedTest
    @MethodSource({"validBlockNumbers", "invalidBlockNumbers"})
    void testSuccessfulFindBlock(final long toResolve) {
        assertThat(toTest.findLiveBlock(toResolve)).isNotNull().isEmpty();
    }

    /**
     * This test aims to verify that the
     * {@link NoOpBlockPathResolver#findArchivedBlock(long)}  always returns an empty
     * optional.
     *
     * @param toResolve parameterized, block number
     */
    @ParameterizedTest
    @MethodSource({"validBlockNumbers", "invalidBlockNumbers"})
    void testSuccessfulFindArchiveBlock(final long toResolve) {
        assertThat(toTest.findArchivedBlock(toResolve)).isNotNull().isEmpty();
    }

    /**
     * This test aims to verify that the
     * {@link NoOpBlockPathResolver#existsVerifiedBlock(long)}
     * always returns false.
     *
     * @param toResolve parameterized, block number
     */
    @ParameterizedTest
    @MethodSource({"validBlockNumbers", "invalidBlockNumbers"})
    void testSuccessfulExistsVerified(final long toResolve) {
        assertThat(toTest.existsVerifiedBlock(toResolve)).isFalse();
    }

    /**
     * This test aims to verify that the
     * {@link NoOpBlockPathResolver#markVerified(long)} does nothing.
     *
     * @param toResolve parameterized, block number
     */
    @ParameterizedTest
    @MethodSource({"validBlockNumbers", "invalidBlockNumbers"})
    void testSuccessfulMarkVerified(final long toResolve, final String blockPath) {
        final Path verifiedBlockPath = Path.of(blockPath);
        final Path unverifiedBlockPath =
                Path.of(blockPath.replace(Constants.BLOCK_FILE_EXTENSION, Constants.UNVERIFIED_BLOCK_FILE_EXTENSION));

        assertThat(verifiedBlockPath).doesNotExist();
        assertThat(unverifiedBlockPath).doesNotExist();

        assertThatCode(() -> toTest.markVerified(toResolve)).doesNotThrowAnyException();

        assertThat(verifiedBlockPath).doesNotExist();
        assertThat(unverifiedBlockPath).doesNotExist();
    }

    /**
     * Some valid block numbers.
     *
     * @return a stream of valid block numbers
     */
    private static Stream<Arguments> validBlockNumbers() {
        return Stream.of(
                Arguments.of(0L, "/tmp/hashgraph/blocknode/data/0.tmp.blk"),
                Arguments.of(1L, "/tmp/hashgraph/blocknode/data/1.tmp.blk"),
                Arguments.of(2L, "/tmp/hashgraph/blocknode/data/2.tmp.blk"),
                Arguments.of(10L, "/tmp/hashgraph/blocknode/data/10.tmp.blk"),
                Arguments.of(100L, "/tmp/hashgraph/blocknode/data/100.tmp.blk"),
                Arguments.of(1_000L, "/tmp/hashgraph/blocknode/data/1000.tmp.blk"),
                Arguments.of(10_000L, "/tmp/hashgraph/blocknode/data/10000.tmp.blk"),
                Arguments.of(100_000L, "/tmp/hashgraph/blocknode/data/100000.tmp.blk"),
                Arguments.of(1_000_000L, "/tmp/hashgraph/blocknode/data/1000000.tmp.blk"),
                Arguments.of(10_000_000L, "/tmp/hashgraph/blocknode/data/10000000.tmp.blk"),
                Arguments.of(100_000_000L, "/tmp/hashgraph/blocknode/data/100000000.tmp.blk"),
                Arguments.of(1_000_000_000L, "/tmp/hashgraph/blocknode/data/1000000000.tmp.blk"),
                Arguments.of(10_000_000_000L, "/tmp/hashgraph/blocknode/data/10000000000.tmp.blk"),
                Arguments.of(100_000_000_000L, "/tmp/hashgraph/blocknode/data/100000000000.tmp.blk"),
                Arguments.of(1_000_000_000_000L, "/tmp/hashgraph/blocknode/data/1000000000000.tmp.blk"),
                Arguments.of(10_000_000_000_000L, "/tmp/hashgraph/blocknode/data/10000000000000.tmp.blk"),
                Arguments.of(100_000_000_000_000L, "/tmp/hashgraph/blocknode/data/100000000000000.tmp.blk"),
                Arguments.of(1_000_000_000_000_000L, "/tmp/hashgraph/blocknode/data/1000000000000000.tmp.blk"),
                Arguments.of(10_000_000_000_000_000L, "/tmp/hashgraph/blocknode/data/10000000000000000.tmp.blk"),
                Arguments.of(100_000_000_000_000_000L, "/tmp/hashgraph/blocknode/data/100000000000000000.tmp.blk"),
                Arguments.of(1_000_000_000_000_000_000L, "/tmp/hashgraph/blocknode/data/1000000000000000000.tmp.blk"),
                Arguments.of(Long.MAX_VALUE, "/tmp/hashgraph/blocknode/data/9223372036854775807.tmp.blk"));
    }

    /**
     * Some invalid block numbers.
     *
     * @return a stream of invalid block numbers
     */
    private static Stream<Arguments> invalidBlockNumbers() {
        return Stream.of(
                Arguments.of(-1L, "/tmp/hashgraph/blocknode/data/-1.tmp.blk"),
                Arguments.of(-2L, "/tmp/hashgraph/blocknode/data/-2.tmp.blk"),
                Arguments.of(-10L, "/tmp/hashgraph/blocknode/data/-10.tmp.blk"),
                Arguments.of(-100L, "/tmp/hashgraph/blocknode/data/-100.tmp.blk"),
                Arguments.of(-1_000L, "/tmp/hashgraph/blocknode/data/-1000.tmp.blk"),
                Arguments.of(-10_000L, "/tmp/hashgraph/blocknode/data/-10000.tmp.blk"),
                Arguments.of(-100_000L, "/tmp/hashgraph/blocknode/data/-100000.tmp.blk"),
                Arguments.of(-1_000_000L, "/tmp/hashgraph/blocknode/data/-1000000.tmp.blk"),
                Arguments.of(-10_000_000L, "/tmp/hashgraph/blocknode/data/-10000000.tmp.blk"),
                Arguments.of(-100_000_000L, "/tmp/hashgraph/blocknode/data/-100000000.tmp.blk"),
                Arguments.of(-1_000_000_000L, "/tmp/hashgraph/blocknode/data/-1000000000.tmp.blk"),
                Arguments.of(-10_000_000_000L, "/tmp/hashgraph/blocknode/data/-10000000000.tmp.blk"),
                Arguments.of(-100_000_000_000L, "/tmp/hashgraph/blocknode/data/-100000000000.tmp.blk"),
                Arguments.of(-1_000_000_000_000L, "/tmp/hashgraph/blocknode/data/-1000000000000.tmp.blk"),
                Arguments.of(-10_000_000_000_000L, "/tmp/hashgraph/blocknode/data/-10000000000000.tmp.blk"),
                Arguments.of(-100_000_000_000_000L, "/tmp/hashgraph/blocknode/data/-100000000000000.tmp.blk"),
                Arguments.of(-1_000_000_000_000_000L, "/tmp/hashgraph/blocknode/data/-1000000000000000.tmp.blk"),
                Arguments.of(-10_000_000_000_000_000L, "/tmp/hashgraph/blocknode/data/-10000000000000000.tmp.blk"),
                Arguments.of(-100_000_000_000_000_000L, "/tmp/hashgraph/blocknode/data/-100000000000000000.tmp.blk"),
                Arguments.of(-1_000_000_000_000_000_000L, "/tmp/hashgraph/blocknode/data/-1000000000000000000.tmp.blk"),
                Arguments.of(Long.MIN_VALUE, "/tmp/hashgraph/blocknode/data/-9223372036854775808.tmp.blk"));
    }
}
