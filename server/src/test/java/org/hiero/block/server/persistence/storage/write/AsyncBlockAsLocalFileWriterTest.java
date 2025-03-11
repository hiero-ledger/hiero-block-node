// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.write;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.BlockPersistenceError;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.BlocksPersisted;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.swirlds.metrics.api.Counter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.stream.Stream;
import org.hiero.block.server.Constants;
import org.hiero.block.server.ack.AckHandler;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.persistence.storage.compression.Compression;
import org.hiero.block.server.persistence.storage.path.BlockPathResolver;
import org.hiero.block.server.persistence.storage.remove.BlockRemover;
import org.hiero.block.server.persistence.storage.write.BlockPersistenceResult.BlockPersistenceStatus;
import org.hiero.block.server.util.PersistTestUtils;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * A test suite for the {@link AsyncBlockAsLocalFileWriter} class.
 */
@ExtendWith(MockitoExtension.class)
class AsyncBlockAsLocalFileWriterTest {
    private static final long TEST_TIMEOUT_MILLIS = 500L;

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

    @Mock
    private Counter successfulPersistenceCounterMock;

    @Mock
    private Counter persistenceErrorCounterMock;

    @TempDir
    private Path testTempDir;

    /**
     * This test aims to verify that the {@link AsyncBlockAsLocalFileWriter#call()}
     * correctly writes a block to the filesystem when supplied with a correct
     * block, no duplicate is found and no problems arose during write.
     *
     * @param validBlockNumber parameterized, valid block number
     */
    @Timeout(value = TEST_TIMEOUT_MILLIS, unit = TimeUnit.MILLISECONDS)
    @ParameterizedTest
    @MethodSource("validBlockNumbers")
    void testSuccessfulWrite(final long validBlockNumber) throws Exception {
        // setup
        final List<BlockItemUnparsed> validBlock =
                PersistTestUtils.generateBlockItemsUnparsedForWithBlockNumber(validBlockNumber);
        final AsyncBlockWriter toTest = new AsyncBlockAsLocalFileWriter(
                validBlockNumber,
                blockPathResolverMock,
                blockRemoverMock,
                compressionMock,
                ackHandlerMock,
                metricsServiceMock);
        final TransferQueue<BlockItemUnparsed> q = toTest.getQueue();
        validBlock.forEach(q::offer);

        // when
        final Path expectedWrittenBlockFile = testTempDir.resolve(validBlockNumber + Constants.BLOCK_FILE_EXTENSION);
        when(blockPathResolverMock.resolveLiveRawUnverifiedPathToBlock(validBlockNumber))
                .thenReturn(expectedWrittenBlockFile);
        when(blockPathResolverMock.existsVerifiedBlock(validBlockNumber)).thenReturn(false);
        when(compressionMock.getCompressionFileExtension()).thenReturn("");
        when(compressionMock.wrap(any(OutputStream.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(metricsServiceMock.get(BlocksPersisted)).thenReturn(successfulPersistenceCounterMock);

        // then
        toTest.call();
        assertThat(expectedWrittenBlockFile)
                .exists()
                .isRegularFile()
                .isReadable()
                .hasBinaryContent(generateByteArrayOfTestBlock(validBlock));
        final BlockPersistenceResult expectedResult =
                new BlockPersistenceResult(validBlockNumber, BlockPersistenceStatus.SUCCESS);
        verifySuccessfulPersistencePublish(expectedResult);
    }

    /**
     * This test aims to verify that the {@link AsyncBlockAsLocalFileWriter#call()}
     * correctly overwrites an already existing block if the call is executed.
     * It is important to assert this since the writer writes blocks as
     * unverified. These blocks are ephemeral and can be overwritten if they
     * fail verification. This test ensures that the writer can overwrite an
     * existing  unverified block. No duplicate is found and no problems arose
     * during write.
     *
     * @param validBlockNumber parameterized, valid block number
     */
    @Timeout(value = TEST_TIMEOUT_MILLIS, unit = TimeUnit.MILLISECONDS)
    @ParameterizedTest
    @MethodSource("validBlockNumbers")
    void testSuccessfulWriteOverwriteExisting(final long validBlockNumber) throws Exception {
        // setup
        final List<BlockItemUnparsed> validBlock =
                PersistTestUtils.generateBlockItemsUnparsedForWithBlockNumber(validBlockNumber);
        final AsyncBlockWriter toTest = new AsyncBlockAsLocalFileWriter(
                validBlockNumber,
                blockPathResolverMock,
                blockRemoverMock,
                compressionMock,
                ackHandlerMock,
                metricsServiceMock);
        final TransferQueue<BlockItemUnparsed> q = toTest.getQueue();
        validBlock.forEach(q::offer);

        // when
        final Path expectedWrittenBlockFile = testTempDir.resolve(validBlockNumber + Constants.BLOCK_FILE_EXTENSION);
        when(blockPathResolverMock.resolveLiveRawUnverifiedPathToBlock(validBlockNumber))
                .thenReturn(expectedWrittenBlockFile);
        when(blockPathResolverMock.existsVerifiedBlock(validBlockNumber)).thenReturn(false);
        when(compressionMock.getCompressionFileExtension()).thenReturn("");
        when(compressionMock.wrap(any(OutputStream.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(metricsServiceMock.get(BlocksPersisted)).thenReturn(successfulPersistenceCounterMock);

        // before call, create the expected file and pollute it with some data
        // we expect that the writer will overwrite this file as it is writing
        // to an unverified block file path that is permitted, since unverified
        // blocks could fail verification so they need to be able to be overwritten
        final String pollution = "block file is polluted";
        Files.writeString(expectedWrittenBlockFile, pollution);
        assertThat(expectedWrittenBlockFile)
                .exists()
                .isRegularFile()
                .isReadable()
                .hasContent(pollution);

        // then (we expect overwrite to happen)
        toTest.call();
        assertThat(expectedWrittenBlockFile)
                .exists()
                .isRegularFile()
                .isReadable()
                .hasBinaryContent(generateByteArrayOfTestBlock(validBlock));
        final BlockPersistenceResult expectedResult =
                new BlockPersistenceResult(validBlockNumber, BlockPersistenceStatus.SUCCESS);
        verifySuccessfulPersistencePublish(expectedResult);
    }

    /**
     * This test aims to verify that the {@link AsyncBlockAsLocalFileWriter#call()}
     * correctly returns a successful result if the offered block is complete,
     * written to the filesystem and no duplicate is found.
     *
     * @param validBlockNumber parameterized, valid block number
     */
    @Timeout(value = TEST_TIMEOUT_MILLIS, unit = TimeUnit.MILLISECONDS)
    @ParameterizedTest
    @MethodSource("validBlockNumbers")
    void testSuccessfulWriteResponse(final long validBlockNumber) throws Exception {
        // setup
        final List<BlockItemUnparsed> validBlock =
                PersistTestUtils.generateBlockItemsUnparsedForWithBlockNumber(validBlockNumber);
        final AsyncBlockWriter toTest = new AsyncBlockAsLocalFileWriter(
                validBlockNumber,
                blockPathResolverMock,
                blockRemoverMock,
                compressionMock,
                ackHandlerMock,
                metricsServiceMock);
        final TransferQueue<BlockItemUnparsed> q = toTest.getQueue();
        validBlock.forEach(q::offer);

        // when
        final Path expectedWrittenBlockFile = testTempDir.resolve(validBlockNumber + Constants.BLOCK_FILE_EXTENSION);
        when(blockPathResolverMock.resolveLiveRawUnverifiedPathToBlock(validBlockNumber))
                .thenReturn(expectedWrittenBlockFile);
        when(blockPathResolverMock.existsVerifiedBlock(validBlockNumber)).thenReturn(false);
        when(compressionMock.getCompressionFileExtension()).thenReturn("");
        when(compressionMock.wrap(any(OutputStream.class))).thenAnswer(invocation -> invocation.getArgument(0));
        when(metricsServiceMock.get(BlocksPersisted)).thenReturn(successfulPersistenceCounterMock);

        // then
        toTest.call();
        final BlockPersistenceResult expectedResult =
                new BlockPersistenceResult(validBlockNumber, BlockPersistenceStatus.SUCCESS);
        verifySuccessfulPersistencePublish(expectedResult);
    }

    /**
     * This test aims to verify that the {@link AsyncBlockAsLocalFileWriter#call()}
     * correctly returns a failure during write status if the offered block is
     * complete, but an exception is thrown during the write operation.
     *
     * @param validBlockNumber parameterized, valid block number
     */
    @Timeout(value = TEST_TIMEOUT_MILLIS, unit = TimeUnit.MILLISECONDS)
    @ParameterizedTest
    @MethodSource("validBlockNumbers")
    void testFailDuringWrite(final long validBlockNumber) throws Exception {
        // setup
        final List<BlockItemUnparsed> validBlock =
                PersistTestUtils.generateBlockItemsUnparsedForWithBlockNumber(validBlockNumber);
        final AsyncBlockWriter toTest = new AsyncBlockAsLocalFileWriter(
                validBlockNumber,
                blockPathResolverMock,
                blockRemoverMock,
                compressionMock,
                ackHandlerMock,
                metricsServiceMock);
        final TransferQueue<BlockItemUnparsed> q = toTest.getQueue();
        validBlock.forEach(q::offer);

        // when
        final Path expectedWrittenBlockFile = testTempDir.resolve(validBlockNumber + Constants.BLOCK_FILE_EXTENSION);
        when(metricsServiceMock.get(BlockPersistenceError)).thenReturn(persistenceErrorCounterMock);
        when(blockPathResolverMock.resolveLiveRawUnverifiedPathToBlock(validBlockNumber))
                .thenReturn(expectedWrittenBlockFile);
        when(blockPathResolverMock.existsVerifiedBlock(validBlockNumber)).thenReturn(false);
        when(compressionMock.getCompressionFileExtension()).thenReturn("");
        when(compressionMock.wrap(any(OutputStream.class))).thenThrow(IOException.class);

        // then
        toTest.call();
        final BlockPersistenceResult expectedResult =
                new BlockPersistenceResult(validBlockNumber, BlockPersistenceStatus.FAILURE_DURING_WRITE);
        verifyUnsuccessfulPersistencePublish(expectedResult);
    }

    /**
     * This test aims to verify that the {@link AsyncBlockAsLocalFileWriter#call()}
     * correctly returns a failure during revert status if the offered block is
     * complete, but an exception is thrown which triggers side effect cleanup
     * which fails.
     *
     * @param validBlockNumber parameterized, valid block number
     */
    @Timeout(value = TEST_TIMEOUT_MILLIS, unit = TimeUnit.MILLISECONDS)
    @ParameterizedTest
    @MethodSource("validBlockNumbers")
    void testFailDuringRevert(final long validBlockNumber) throws Exception {
        // setup
        final List<BlockItemUnparsed> validBlock =
                PersistTestUtils.generateBlockItemsUnparsedForWithBlockNumber(validBlockNumber);
        final AsyncBlockWriter toTest = new AsyncBlockAsLocalFileWriter(
                validBlockNumber,
                blockPathResolverMock,
                blockRemoverMock,
                compressionMock,
                ackHandlerMock,
                metricsServiceMock);
        final TransferQueue<BlockItemUnparsed> q = toTest.getQueue();
        validBlock.forEach(q::offer);

        // when
        final Path expectedWrittenBlockFile = testTempDir.resolve(validBlockNumber + Constants.BLOCK_FILE_EXTENSION);
        when(blockPathResolverMock.resolveLiveRawUnverifiedPathToBlock(validBlockNumber))
                .thenReturn(expectedWrittenBlockFile);
        when(blockPathResolverMock.existsVerifiedBlock(validBlockNumber)).thenReturn(false);
        when(compressionMock.getCompressionFileExtension()).thenReturn("");
        when(compressionMock.wrap(any(OutputStream.class))).thenThrow(IOException.class);
        when(blockRemoverMock.removeUnverified(validBlockNumber)).thenThrow(IOException.class);
        when(metricsServiceMock.get(BlockPersistenceError)).thenReturn(persistenceErrorCounterMock);

        // then
        toTest.call();
        final BlockPersistenceResult expectedResult =
                new BlockPersistenceResult(validBlockNumber, BlockPersistenceStatus.FAILURE_DURING_REVERT);
        verifyUnsuccessfulPersistencePublish(expectedResult);
    }

    /**
     * This test aims to verify that the {@link AsyncBlockAsLocalFileWriter#call()}
     * correctly returns an incomplete block status if the offered block is
     * incomplete, meaning the incomplete block header flag has been offered to
     * the queue of the writer. This would generally happen if the handler
     * receives a block header before it has received a proof to complete the
     * current block, and it would supply the flag to the writer to indicate that
     * the block is incomplete. The writer should return an incomplete block
     * status in this case and cleanup any side effects that may have occurred
     * during the writing of the block.
     *
     * @param validBlockNumber parameterized, valid block number
     */
    @Timeout(value = TEST_TIMEOUT_MILLIS, unit = TimeUnit.MILLISECONDS)
    @ParameterizedTest
    @MethodSource("validBlockNumbers")
    void testIncompleteBlockFlag(final long validBlockNumber) throws Exception {
        // setup
        final List<BlockItemUnparsed> validBlock =
                PersistTestUtils.generateBlockItemsUnparsedForWithBlockNumber(validBlockNumber);

        when(metricsServiceMock.get(BlockPersistenceError)).thenReturn(persistenceErrorCounterMock);
        final AsyncBlockWriter toTest = new AsyncBlockAsLocalFileWriter(
                validBlockNumber,
                blockPathResolverMock,
                blockRemoverMock,
                compressionMock,
                ackHandlerMock,
                metricsServiceMock);
        final TransferQueue<BlockItemUnparsed> q = toTest.getQueue();
        validBlock.removeLast();
        validBlock.addLast(AsyncBlockWriter.INCOMPLETE_BLOCK_FLAG);
        validBlock.forEach(q::offer);

        // when
        when(blockPathResolverMock.existsVerifiedBlock(validBlockNumber)).thenReturn(false);

        // then
        toTest.call();
        final BlockPersistenceResult expectedResult =
                new BlockPersistenceResult(validBlockNumber, BlockPersistenceStatus.INCOMPLETE_BLOCK);
        verifyUnsuccessfulPersistencePublish(expectedResult);
    }

    /**
     * This test aims to verify that the {@link AsyncBlockAsLocalFileWriter#call()}
     * correctly returns a duplicate block result if the offered block is already
     * persisted and verified.
     *
     * @param validBlockNumber parameterized, valid block number
     */
    @Timeout(value = TEST_TIMEOUT_MILLIS, unit = TimeUnit.MILLISECONDS)
    @ParameterizedTest
    @MethodSource("validBlockNumbers")
    void testExistingVerifiedBlockNumber(final long validBlockNumber) throws Exception {
        // setup
        final List<BlockItemUnparsed> validBlock =
                PersistTestUtils.generateBlockItemsUnparsedForWithBlockNumber(validBlockNumber);
        final AsyncBlockWriter toTest = new AsyncBlockAsLocalFileWriter(
                validBlockNumber,
                blockPathResolverMock,
                blockRemoverMock,
                compressionMock,
                ackHandlerMock,
                metricsServiceMock);
        final TransferQueue<BlockItemUnparsed> q = toTest.getQueue();
        validBlock.forEach(q::offer);

        // when
        when(blockPathResolverMock.existsVerifiedBlock(validBlockNumber)).thenReturn(true);
        when(metricsServiceMock.get(BlockPersistenceError)).thenReturn(persistenceErrorCounterMock);

        // then
        toTest.call();
        final BlockPersistenceResult expectedResult =
                new BlockPersistenceResult(validBlockNumber, BlockPersistenceStatus.DUPLICATE_BLOCK);
        verifyUnsuccessfulPersistencePublish(expectedResult);
    }

    /**
     * This test aims to verify that we cannot create an instance of
     * {@link AsyncBlockAsLocalFileWriter} with an invalid block number.
     *
     * @param invalidBlockNumber parameterized, invalid block number
     */
    @Timeout(value = TEST_TIMEOUT_MILLIS, unit = TimeUnit.MILLISECONDS)
    @ParameterizedTest
    @MethodSource("invalidBlockNumbers")
    void testConstructorInvalidBlockNumber(final long invalidBlockNumber) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new AsyncBlockAsLocalFileWriter(
                        invalidBlockNumber,
                        blockPathResolverMock,
                        blockRemoverMock,
                        compressionMock,
                        ackHandlerMock,
                        metricsServiceMock));
    }

    /**
     * Helper to generate a byte array of a test block in order to compare the
     * written block to the expected block.
     */
    private byte[] generateByteArrayOfTestBlock(final List<BlockItemUnparsed> validBlock) {
        final BlockUnparsed blockUnparsed =
                BlockUnparsed.newBuilder().blockItems(validBlock).build();
        return BlockUnparsed.PROTOBUF.toBytes(blockUnparsed).toByteArray();
    }

    /**
     * Writers should always publish a result themselves when they finish their
     * job successfully. We verify that the result is published to the ack handler
     * and the metrics service counter is incremented.
     */
    private void verifySuccessfulPersistencePublish(final BlockPersistenceResult actual) {
        verify(ackHandlerMock, times(1)).blockPersisted(actual);
        verify(metricsServiceMock, times(1)).get(BlocksPersisted);
        verify(successfulPersistenceCounterMock, times(1)).increment();
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
        verify(successfulPersistenceCounterMock, never()).increment();
        verify(persistenceErrorCounterMock, times(1)).increment();
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
