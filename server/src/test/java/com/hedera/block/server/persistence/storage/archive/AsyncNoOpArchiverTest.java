// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.archive;

import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link AsyncNoOpArchiver}.
 */
class AsyncNoOpArchiverTest {
    /**
     * This test aims to assert that the {@link AsyncNoOpArchiver} does
     * absolutely nothing and does not throw any exceptions no matter if valid
     * or invalid thresholds are provided.
     */
    @ParameterizedTest
    @MethodSource({"validThresholds", "invalidThresholds"})
    void testArchiveBlock(final long threshold) {
        final AsyncNoOpArchiver archiver = new AsyncNoOpArchiver(threshold);
        assertThatCode(archiver::run).doesNotThrowAnyException();
    }

    private static Stream<Arguments> validThresholds() {
        return Stream.of(Arguments.of(10L), Arguments.of(100L), Arguments.of(1000L));
    }

    private static Stream<Arguments> invalidThresholds() {
        return Stream.of(Arguments.of(-1L), Arguments.of(0L), Arguments.of(1L), Arguments.of(2L), Arguments.of(25L));
    }
}
