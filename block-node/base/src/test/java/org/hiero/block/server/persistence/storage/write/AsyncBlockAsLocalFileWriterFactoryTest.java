// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.write;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.stream.Stream;
import org.hiero.block.server.ack.AckHandler;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.persistence.storage.compression.Compression;
import org.hiero.block.server.persistence.storage.path.BlockPathResolver;
import org.hiero.block.server.persistence.storage.remove.BlockRemover;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test class for {@link AsyncBlockAsLocalFileWriterFactory}
 */
@ExtendWith(MockitoExtension.class)
class AsyncBlockAsLocalFileWriterFactoryTest {
    @Mock
    private BlockPathResolver blockPathResolverMock;

    @Mock
    private BlockRemover blockRemoverMock;

    @Mock
    private Compression compressionMock;

    @Mock
    private AckHandler ackHandlerMock;

    @Mock
    private MetricsService metricsServiceMock;

    private AsyncBlockAsLocalFileWriterFactory toTest;

    @BeforeEach
    void setUp() {
        toTest = new AsyncBlockAsLocalFileWriterFactory(
                blockPathResolverMock, blockRemoverMock, compressionMock, ackHandlerMock, metricsServiceMock);
    }

    /**
     * This test aims to verify that the
     * {@link AsyncBlockAsLocalFileWriterFactory#create(long)} correctly
     * creates an {@link AsyncBlockAsLocalFileWriter} instance.
     *
     * @param blockNumber parameterized, block number
     */
    @ParameterizedTest
    @MethodSource("validBlockNumbers")
    void testCreate(final long blockNumber) {
        final AsyncBlockWriter actual = toTest.create(blockNumber);
        assertThat(actual).isNotNull().isExactlyInstanceOf(AsyncBlockAsLocalFileWriter.class);
    }

    /**
     * This test aims to verify that the
     * {@link AsyncBlockAsLocalFileWriterFactory#create(long)} correctly
     * throws an {@link IllegalArgumentException} when an invalid block number is
     * provided.
     *
     * @param blockNumber parameterized, block number
     */
    @ParameterizedTest
    @MethodSource("invalidBlockNumbers")
    void testCreateInvalidBlockNumber(final long blockNumber) {
        assertThatIllegalArgumentException().isThrownBy(() -> toTest.create(blockNumber));
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
