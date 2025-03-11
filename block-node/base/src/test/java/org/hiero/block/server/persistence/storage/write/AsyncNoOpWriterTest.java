// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.write;

import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.BlocksPersisted;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.BlockItemUnparsed;
import com.swirlds.metrics.api.Counter;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.stream.Stream;
import org.hiero.block.server.ack.AckHandler;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.persistence.storage.write.BlockPersistenceResult.BlockPersistenceStatus;
import org.hiero.block.server.util.PersistTestUtils;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test class for {@link AsyncNoOpWriter}.
 */
@ExtendWith(MockitoExtension.class)
class AsyncNoOpWriterTest {
    private static final long TEST_TIMEOUT_MILLIS = 500L;

    @Mock
    private AckHandler ackHandlerMock;

    @Mock
    private MetricsService metricsServiceMock;

    @Mock
    private Counter counterMock;

    /**
     * This test aims to verify that the
     * {@link AsyncNoOpWriter#call()} correctly
     * returns a successful result if the offered block is complete. No
     * preconditions check for block number.
     *
     * @param blockNumber parameterized, block number
     */
    @Timeout(value = TEST_TIMEOUT_MILLIS, unit = TimeUnit.MILLISECONDS)
    @ParameterizedTest
    @MethodSource({"validBlockNumbers", "invalidBlockNumbers"})
    void testWrite(final long blockNumber) {
        // setup
        final List<BlockItemUnparsed> currentBlock =
                PersistTestUtils.generateBlockItemsUnparsedForWithBlockNumber(blockNumber);
        final AsyncNoOpWriter toTest = new AsyncNoOpWriter(blockNumber, ackHandlerMock, metricsServiceMock);
        final TransferQueue<BlockItemUnparsed> q = toTest.getQueue();
        currentBlock.forEach(q::offer);

        // when
        when(metricsServiceMock.get(BlocksPersisted)).thenReturn(counterMock);

        // then
        toTest.call();
        final BlockPersistenceResult expectedResult =
                new BlockPersistenceResult(blockNumber, BlockPersistenceStatus.SUCCESS);
        verifySuccessfulPersistencePublish(expectedResult);
    }

    /**
     * This test aims to verify that the
     * {@link AsyncNoOpWriter#call()} correctly
     * returns an incomplete block status if the offered block is incomplete. No
     * preconditions check for block number.
     *
     * @param blockNumber parameterized, block number
     */
    @Timeout(value = TEST_TIMEOUT_MILLIS, unit = TimeUnit.MILLISECONDS)
    @ParameterizedTest
    @MethodSource({"validBlockNumbers", "invalidBlockNumbers"})
    void testWriteIncompleteBlock(final long blockNumber) {
        // setup
        final List<BlockItemUnparsed> currentBlock =
                PersistTestUtils.generateBlockItemsUnparsedForWithBlockNumber(blockNumber);
        currentBlock.removeLast();
        currentBlock.addLast(AsyncBlockWriter.INCOMPLETE_BLOCK_FLAG);
        final AsyncNoOpWriter toTest = new AsyncNoOpWriter(blockNumber, ackHandlerMock, metricsServiceMock);
        final TransferQueue<BlockItemUnparsed> q = toTest.getQueue();
        currentBlock.forEach(q::offer);

        // then
        toTest.call();
        final BlockPersistenceResult expectedResult =
                new BlockPersistenceResult(blockNumber, BlockPersistenceStatus.INCOMPLETE_BLOCK);
        verifyUnsuccessfulPersistencePublish(expectedResult);
    }

    /**
     * Writers should always publish a result themselves when they finish their
     * job successfully. We verify that the result is published to the ack handler
     * and the metrics service counter is incremented.
     */
    private void verifySuccessfulPersistencePublish(final BlockPersistenceResult actual) {
        verify(ackHandlerMock, times(1)).blockPersisted(actual);
        verify(metricsServiceMock, times(1)).get(BlocksPersisted);
        verify(counterMock, times(1)).increment();
    }

    /**
     * Writers should always publish a result themselves when they finish their
     * job unsuccessfully. We verify that the result is published to the ack handler
     * and the metrics service counter is never interacted with, no counters are
     * incremented.
     */
    private void verifyUnsuccessfulPersistencePublish(final BlockPersistenceResult actual) {
        verify(ackHandlerMock, times(1)).blockPersisted(actual);
        verify(metricsServiceMock, never()).get(BlocksPersisted);
        verify(counterMock, never()).increment();
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
