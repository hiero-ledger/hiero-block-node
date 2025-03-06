// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.remove;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test class for {@link NoOpBlockRemover}.
 */
class NoOpBlockRemoverTest {
    private NoOpBlockRemover toTest;

    @BeforeEach
    void setUp() {
        toTest = NoOpBlockRemover.newInstance();
    }

    /**
     * This test aims to verify that the
     * {@link NoOpBlockRemover#removeLiveUnverified(long)} does nothing and
     * returns false always. The no-op remover has no preconditions check as well.
     *
     * @param toRemove parameterized, block number
     */
    @ParameterizedTest
    @MethodSource({"validBlockNumbers", "invalidBlockNumbers"})
    void testSuccessfulBlockDeletion(final long toRemove) {
        final boolean actual = toTest.removeLiveUnverified(toRemove);
        assertThat(actual).isFalse();
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
