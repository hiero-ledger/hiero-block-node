// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.verification.session;

import static org.hiero.block.common.utils.FileUtilities.readGzipFileUnsafe;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.VerificationBlockTime;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.VerificationBlocksError;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.VerificationBlocksFailed;
import static org.hiero.block.server.metrics.BlockNodeMetricTypes.Counter.VerificationBlocksVerified;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.hedera.hapi.block.BlockItemUnparsed;
import com.hedera.hapi.block.BlockUnparsed;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Counter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.block.server.verification.BlockVerificationStatus;
import org.hiero.block.server.verification.VerificationResult;
import org.hiero.block.server.verification.signature.SignatureVerifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public abstract class BlockVerificationSessionBaseTest {

    @Mock
    protected MetricsService metricsService;

    @Mock
    protected SignatureVerifier signatureVerifier;

    @Mock
    protected Counter verificationBlocksVerified;

    @Mock
    protected Counter verificationBlocksFailed;

    @Mock
    protected Counter verificationBlockTime;

    @Mock
    protected Counter verificationBlocksError;

    protected final Bytes hashing01BlockHash = Bytes.fromHex(
            "24ec308ac4b552c83fcde20ba443bf7b69ae435f8e74f09086bfb339151f65a7c6f06eb2bdc4c50b69a12685f6168e84");

    @BeforeEach
    void setUpBase() {
        MockitoAnnotations.openMocks(this);
        when(metricsService.get(VerificationBlocksVerified)).thenReturn(verificationBlocksVerified);
        when(metricsService.get(VerificationBlocksFailed)).thenReturn(verificationBlocksFailed);
        when(metricsService.get(VerificationBlockTime)).thenReturn(verificationBlockTime);
        when(metricsService.get(VerificationBlocksError)).thenReturn(verificationBlocksError);
    }

    protected abstract BlockVerificationSession createSession(BlockHeader blockHeader);

    protected List<BlockItemUnparsed> getTestBlock1Items() throws IOException, ParseException, URISyntaxException {
        Path block01Path =
                Path.of(getClass().getResource("/test-blocks/hashing-01.blk.gz").toURI());
        Bytes block01Bytes = Bytes.wrap(readGzipFileUnsafe(block01Path));
        BlockUnparsed blockUnparsed = BlockUnparsed.PROTOBUF.parse(block01Bytes);
        return blockUnparsed.blockItems();
    }

    @Test
    void testSuccessfulVerification() throws Exception {
        // Given
        List<BlockItemUnparsed> blockItems = getTestBlock1Items();
        BlockHeader blockHeader =
                BlockHeader.PROTOBUF.parse(blockItems.getFirst().blockHeader());
        BlockVerificationSession session = createSession(blockHeader);

        when(signatureVerifier.verifySignature(any(Bytes.class), any(Bytes.class)))
                .thenReturn(true);

        // When
        session.appendBlockItems(blockItems);
        CompletableFuture<VerificationResult> future = session.getVerificationResult();
        VerificationResult result = future.get();

        // Then
        assertEquals(BlockVerificationStatus.VERIFIED, result.status());
        assertEquals(1L, result.blockNumber());
        assertEquals(hashing01BlockHash, result.blockHash());
        assertFalse(session.isRunning());
        verify(verificationBlocksVerified, times(1)).increment();
        verify(verificationBlockTime, times(1)).add(any(Long.class));
        verifyNoMoreInteractions(verificationBlocksFailed);
    }

    @Test
    void testSuccessfulVerification_multipleAppends() throws Exception {
        // Given
        List<BlockItemUnparsed> blockItems = getTestBlock1Items();
        // Slice list into 2 parts of different sizes
        List<BlockItemUnparsed> blockItems1 = blockItems.subList(0, 3);
        List<BlockItemUnparsed> blockItems2 = blockItems.subList(3, blockItems.size());
        BlockHeader blockHeader =
                BlockHeader.PROTOBUF.parse(blockItems.getFirst().blockHeader());
        BlockVerificationSession session = createSession(blockHeader);
        when(signatureVerifier.verifySignature(any(Bytes.class), any(Bytes.class)))
                .thenReturn(true);

        // When
        session.appendBlockItems(blockItems1);
        session.appendBlockItems(blockItems2);
        CompletableFuture<VerificationResult> future = session.getVerificationResult();
        VerificationResult result = future.get();

        // Then
        assertEquals(BlockVerificationStatus.VERIFIED, result.status());
        assertEquals(1L, result.blockNumber());
        assertEquals(hashing01BlockHash, result.blockHash());
        assertFalse(session.isRunning());
        verify(verificationBlocksVerified, times(1)).increment();
        verify(verificationBlockTime, times(1)).add(any(Long.class));
        verifyNoMoreInteractions(verificationBlocksFailed);
    }

    @Test
    void testVerificationFailure() throws Exception {
        // Given
        List<BlockItemUnparsed> blockItems = getTestBlock1Items();
        final Bytes hashing01BlockHash = Bytes.fromHex(
                "24ec308ac4b552c83fcde20ba443bf7b69ae435f8e74f09086bfb339151f65a7c6f06eb2bdc4c50b69a12685f6168e84");
        BlockHeader blockHeader =
                BlockHeader.PROTOBUF.parse(blockItems.getFirst().blockHeader());
        BlockVerificationSession session = createSession(blockHeader);
        when(signatureVerifier.verifySignature(any(Bytes.class), any(Bytes.class)))
                .thenReturn(false);

        // When
        session.appendBlockItems(blockItems);
        CompletableFuture<VerificationResult> future = session.getVerificationResult();
        VerificationResult result = future.get();

        // Then
        assertEquals(BlockVerificationStatus.INVALID_HASH_OR_SIGNATURE, result.status());
        assertEquals(1L, result.blockNumber());
        assertEquals(hashing01BlockHash, result.blockHash());
        assertFalse(session.isRunning());
        verifyNoMoreInteractions(verificationBlocksVerified);
        verify(verificationBlocksFailed, times(1)).increment();
    }

    @Test
    void testAppendBlockItemsNotRunning() throws Exception {
        // Given
        List<BlockItemUnparsed> blockItems = getTestBlock1Items();
        BlockHeader blockHeader =
                BlockHeader.PROTOBUF.parse(blockItems.getFirst().blockHeader());
        BlockVerificationSession session = createSession(blockHeader);
        when(signatureVerifier.verifySignature(any(Bytes.class), any(Bytes.class)))
                .thenReturn(true);
        // send a whole block and wait for the result, the session should be completed.
        session.appendBlockItems(blockItems);
        CompletableFuture<VerificationResult> future = session.getVerificationResult();
        VerificationResult result = future.get();

        // metrics should be 1
        verify(verificationBlocksVerified, times(1)).increment();
        verify(verificationBlockTime, times(1)).add(any(Long.class));

        // When
        // Try to append more items after the session has completed
        session.appendBlockItems(blockItems);

        // Then
        // counters should still be 1
        verify(verificationBlocksVerified, times(1)).increment();
        verify(verificationBlockTime, times(1)).add(any(Long.class));
    }

    @Test
    void testParseException()
            throws IOException, ParseException, URISyntaxException, ExecutionException, InterruptedException {
        // Given
        List<BlockItemUnparsed> blockItems = getTestBlock1Items();
        BlockHeader blockHeader =
                BlockHeader.PROTOBUF.parse(blockItems.getFirst().blockHeader());
        blockItems.set(
                blockItems.size() - 1,
                BlockItemUnparsed.newBuilder().blockProof(Bytes.wrap("invalid")).build());
        BlockVerificationSession session = createSession(blockHeader);

        // When
        session.appendBlockItems(blockItems);
        CompletableFuture<VerificationResult> future = session.getVerificationResult();
        assertThrows(ExecutionException.class, future::get);

        // Then
        assertTrue(future.isCompletedExceptionally());
        verify(verificationBlocksError, times(1)).increment();
    }
}
