// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.verification;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;

import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Counter;
import java.util.List;
import org.hiero.block.node.verification.service.BlockVerificationService;
import org.hiero.block.node.verification.service.BlockVerificationServiceImpl;
import org.hiero.block.node.verification.session.BlockVerificationSession;
import org.hiero.block.node.verification.session.BlockVerificationSessionFactory;
import org.hiero.block.server.ack.AckHandler;
import org.hiero.block.server.metrics.BlockNodeMetricTypes;
import org.hiero.block.server.metrics.MetricsService;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class BlockVerificationServiceImplTest {

    @Mock
    private MetricsService metricsService;

    @Mock
    private BlockVerificationSessionFactory sessionFactory;

    @Mock
    private BlockVerificationSession previousSession;

    @Mock
    private BlockVerificationSession newSession;

    @Mock
    private Counter verificationBlocksReceived;

    @Mock
    private Counter verificationBlocksFailed;

    @Mock
    private AckHandler ackHandlerMock;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(metricsService.get(BlockNodeMetricTypes.Counter.VerificationBlocksReceived))
                .thenReturn(verificationBlocksReceived);
        when(metricsService.get(BlockNodeMetricTypes.Counter.VerificationBlocksFailed))
                .thenReturn(verificationBlocksFailed);

        ackHandlerMock = mock(AckHandler.class);
    }

    @Test
    void testOnBlockItemsReceivedNoBlockHeaderNoCurrentSession() throws ParseException {
        BlockItemUnparsed normalItem = getNormalBlockItem();
        List<BlockItemUnparsed> blockItems = List.of(normalItem);

        BlockVerificationService service =
                new BlockVerificationServiceImpl(metricsService, sessionFactory, ackHandlerMock);

        // When
        IllegalStateException exception =
                assertThrows(IllegalStateException.class, () -> service.onBlockItemsReceived(blockItems));

        // Then
        verifyNoInteractions(sessionFactory);
        verifyNoInteractions(verificationBlocksReceived, verificationBlocksFailed);
        assertEquals("Received block items before a block header.", exception.getMessage());
    }

    @Test
    void testOnBlockItemsReceivedNoBlockHeaderWithCurrentSession() throws ParseException {
        BlockItemUnparsed normalItem = getNormalBlockItem();
        List<BlockItemUnparsed> blockItems = List.of(normalItem);

        BlockVerificationServiceImpl service =
                new BlockVerificationServiceImpl(metricsService, sessionFactory, ackHandlerMock);
        setCurrentSession(service, previousSession);

        // When
        service.onBlockItemsReceived(blockItems);

        // Then
        verify(previousSession).appendBlockItems(blockItems);
        verifyNoInteractions(verificationBlocksReceived, verificationBlocksFailed);
    }

    private VerificationResult getVerificationResult(long blockNumber) {
        return new VerificationResult(
                blockNumber, Bytes.wrap(("hash" + blockNumber).getBytes()), BlockVerificationStatus.VERIFIED);
    }

    private BlockHeader getBlockHeader(long blockNumber) {
        long previousBlockNumber = blockNumber - 1;

        return BlockHeader.newBuilder()
                .previousBlockHash(Bytes.wrap(("hash" + previousBlockNumber).getBytes()))
                .number(blockNumber)
                .build();
    }

    private BlockItemUnparsed getBlockHeaderUnparsed(long blockNumber) {
        return BlockItemUnparsed.newBuilder()
                .blockHeader(BlockHeader.PROTOBUF.toBytes(getBlockHeader(blockNumber)))
                .build();
    }

    private BlockItemUnparsed getNormalBlockItem() {
        // A block item without a block header
        return BlockItemUnparsed.newBuilder().build();
    }

    // Helper method to set the currentSession field via reflection since it’s private
    private static void setCurrentSession(BlockVerificationServiceImpl service, BlockVerificationSession session) {
        try {
            var field = BlockVerificationServiceImpl.class.getDeclaredField("currentSession");
            field.setAccessible(true);
            field.set(service, session);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            fail("Unable to set currentSession via reflection", e);
        }
    }
}
