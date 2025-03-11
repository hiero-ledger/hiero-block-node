// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.write;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.hiero.block.server.ack.AckHandler;
import org.hiero.block.server.metrics.MetricsService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test class for {@link AsyncNoOpWriterFactory}.
 */
@ExtendWith(MockitoExtension.class)
class AsyncNoOpWriterFactoryTest {
    @Mock
    private AckHandler ackHandlerMock;

    @Mock
    private MetricsService metricsServiceMock;

    private AsyncNoOpWriterFactory toTest;

    @BeforeEach
    void setUp() {
        toTest = new AsyncNoOpWriterFactory(ackHandlerMock, metricsServiceMock);
    }

    /**
     * This test aims to verify that the
     * {@link AsyncNoOpWriterFactory#create(long)} correctly
     * creates an {@link AsyncNoOpWriterFactory} instance, no precondition
     * check for the block number.
     *
     * @param blockNumber parameterized, block number
     */
    @ParameterizedTest
    @MethodSource({"validBlockNumbers", "invalidBlockNumbers"})
    void testCreate(final long blockNumber) {
        final AsyncBlockWriter actual = toTest.create(blockNumber);
        assertThat(actual).isNotNull().isExactlyInstanceOf(AsyncNoOpWriter.class);
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
