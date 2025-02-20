// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server.persistence.storage.archive;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link AsyncNoOpArchiverFactory}.
 */
class AsyncNoOpArchiverFactoryTest {
    /**
     * This test aims to assert that the {@link AsyncNoOpArchiverFactory} creates
     * an instance of {@link AsyncNoOpArchiver} no matter if valid or invalid
     * thresholds are provided.
     */
    @ParameterizedTest
    @MethodSource({"validThresholds", "invalidThresholds"})
    void testCreate(final long threshold) {
        final AsyncNoOpArchiverFactory toTest = new AsyncNoOpArchiverFactory();
        final AsyncLocalBlockArchiver actual = toTest.create(threshold);
        assertThat(actual).isNotNull().isExactlyInstanceOf(AsyncNoOpArchiver.class);
    }

    private static Stream<Arguments> validThresholds() {
        return Stream.of(Arguments.of(10L), Arguments.of(100L), Arguments.of(1000L));
    }

    private static Stream<Arguments> invalidThresholds() {
        return Stream.of(Arguments.of(-1L), Arguments.of(0L), Arguments.of(1L), Arguments.of(2L), Arguments.of(25L));
    }
}
