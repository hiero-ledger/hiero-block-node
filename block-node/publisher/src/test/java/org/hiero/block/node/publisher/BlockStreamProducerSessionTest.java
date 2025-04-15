// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.publisher;

import static org.hiero.block.node.app.fixtures.blocks.BlockItemUtils.toBlockItemUnparsed;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.sampleBlockHeader;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.sampleBlockProof;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.sampleRoundHeader;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.hedera.pbj.runtime.grpc.Pipeline;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.metrics.api.Counter;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.hapi.block.node.BlockItemUnparsed;
import org.hiero.hapi.block.node.PublishStreamResponse;
import org.hiero.hapi.block.node.PublishStreamResponse.ResponseOneOfType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for {@link BlockStreamProducerSession}.
 * Tests cover session state management, block processing, and response handling.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Block Stream Producer Session Tests")
public class BlockStreamProducerSessionTest {

    @Mock
    private UpdateCallback onUpdate;

    @Mock
    private Consumer<BlockItems> sendToBlockMessaging;

    @Mock
    private Counter liveBlockItemsReceived;

    private BlockStreamProducerSession session;

    private PublishStreamResponse lastResponse;

    private final BlockItemUnparsed SAMPLE_BLOCK_HEADER = toBlockItemUnparsed(sampleBlockHeader(0));
    private final BlockItemUnparsed SAMPLE_ROUND_HEADER = toBlockItemUnparsed(sampleRoundHeader(0));
    private final BlockItemUnparsed SAMPLE_BLOCK_PROOF = toBlockItemUnparsed(sampleBlockProof(0));

    @BeforeEach
    void setUp() {
        final ReentrantLock stateLock = new ReentrantLock();
        Pipeline<? super PublishStreamResponse> responsePipeline = new ResponsePipeline();
        session = new BlockStreamProducerSession(
                1L, // sessionId
                responsePipeline,
                onUpdate,
                liveBlockItemsReceived,
                stateLock,
                sendToBlockMessaging);
    }

    /**
     * Tests the initial state of a new session.
     * Verifies that the session starts with correct default values.
     */
    @Test
    @DisplayName("Should initialize with correct default values")
    void testInitialState() {
        assertEquals(1L, session.sessionId());
        assertEquals(BlockStreamProducerSession.BlockState.NEW, session.currentBlockState());
        assertEquals(UNKNOWN_BLOCK_NUMBER, session.currentBlockNumber());
        assertEquals(0L, session.startTimeOfCurrentBlock());
    }

    /**
     * Tests the state transition from NEW to PRIMARY.
     * Verifies that the session correctly handles becoming the primary session.
     */
    @Test
    @DisplayName("Should transition from NEW to PRIMARY state")
    void testSwitchToPrimary() {
        // Initial state should be NEW
        assertEquals(BlockStreamProducerSession.BlockState.NEW, session.currentBlockState());

        // Switch to primary
        session.switchToPrimary();

        // Verify state change
        assertEquals(BlockStreamProducerSession.BlockState.PRIMARY, session.currentBlockState());
    }

    /**
     * Tests the state transition from NEW to BEHIND.
     * Verifies that the session correctly handles becoming a behind session.
     */
    @Test
    @DisplayName("Should transition from NEW to BEHIND state")
    void testSwitchToBehind() {
        // Initial state should be NEW
        assertEquals(BlockStreamProducerSession.BlockState.NEW, session.currentBlockState());

        // Switch to behind
        session.switchToBehind();

        // Verify state change
        assertEquals(BlockStreamProducerSession.BlockState.BEHIND, session.currentBlockState());
        assertNotNull(lastResponse);
        assertEquals(ResponseOneOfType.SKIP_BLOCK, lastResponse.response().kind());
    }

    /**
     * Tests handling of block items in different states.
     * Verifies that the session correctly processes items based on its current state.
     */
    @Test
    @DisplayName("Should handle block items correctly in different states")
    void testBlockItemProcessing() {
        // Test processing in NEW state with block header
        session.onNext(List.of(SAMPLE_BLOCK_HEADER));
        assertEquals(BlockStreamProducerSession.BlockState.NEW, session.currentBlockState());
        assertEquals(0L, session.currentBlockNumber());

        // Test processing in NEW state with transaction result
        session.onNext(List.of(SAMPLE_ROUND_HEADER));
        assertEquals(BlockStreamProducerSession.BlockState.NEW, session.currentBlockState());

        // Test processing in PRIMARY state with transaction result
        session.switchToPrimary();
        session.onNext(List.of(SAMPLE_ROUND_HEADER));
        assertEquals(BlockStreamProducerSession.BlockState.PRIMARY, session.currentBlockState());
    }

    /**
     * Tests handling of block items in different states.
     * Verifies that the session correctly processes items based on its current state.
     */
    @Test
    @DisplayName("Should handle empty block items list correctly")
    void testEmptyBlockItemProcessing() {
        session.onNext(List.of(SAMPLE_BLOCK_HEADER));
        assertEquals(BlockStreamProducerSession.BlockState.NEW, session.currentBlockState());
        assertEquals(0L, session.currentBlockNumber());

        session.onNext(List.of());
        assertEquals(BlockStreamProducerSession.BlockState.WAITING_FOR_RESEND, session.currentBlockState());
        assertEquals(0L, session.currentBlockNumber());
    }

    /**
     * Tests handling of block header parsing and state transitions.
     * Verifies that the session correctly handles block headers and updates state accordingly.
     */
    @Test
    @DisplayName("Should handle block header parsing and state transitions")
    void testBlockHeaderParsing() {
        // Test with valid block header
        session.onNext(List.of(SAMPLE_BLOCK_HEADER));
        assertEquals(BlockStreamProducerSession.BlockState.NEW, session.currentBlockState());
        assertEquals(0L, session.currentBlockNumber());

        // Test with invalid block header (should throw ParseException)
        assertThrows(
                RuntimeException.class,
                () -> session.onNext(List.of(BlockItemUnparsed.newBuilder()
                        .blockHeader(Bytes.fromHex("invalid"))
                        .build())));
    }

    /**
     * Tests handling of block proof and state transitions.
     * Verifies that the session correctly handles block proofs and updates state accordingly.
     */
    @Test
    @DisplayName("Should handle block proof and state transitions")
    void testBlockProofHandling() {
        // Start with block header
        session.onNext(List.of(SAMPLE_BLOCK_HEADER));

        // Add round header
        session.onNext(List.of(SAMPLE_ROUND_HEADER));

        // Add block proof
        session.onNext(List.of(SAMPLE_BLOCK_PROOF));

        // Verify state transition back to NEW after block proof
        assertEquals(BlockStreamProducerSession.BlockState.NEW, session.currentBlockState());
        assertEquals(0L, session.currentBlockNumber());
    }

    /**
     * Tests handling of block items in WAITING_FOR_RESEND state.
     * Verifies that the session correctly ignores items while waiting for resend.
     */
    @Test
    @DisplayName("Should handle block items in WAITING_FOR_RESEND state")
    void testWaitingForResendState() {
        // Set up waiting for resend state
        session.requestResend(100L);

        // Send items while waiting for resend
        session.onNext(List.of(SAMPLE_ROUND_HEADER));

        // Verify state remains WAITING_FOR_RESEND
        assertEquals(BlockStreamProducerSession.BlockState.WAITING_FOR_RESEND, session.currentBlockState());
        assertEquals(100L, session.currentBlockNumber());
    }

    /**
     * Tests handling of block items in DISCONNECTED state.
     * Verifies that the session correctly handles items while disconnected.
     */
    @Test
    @DisplayName("Should handle block items in DISCONNECTED state")
    void testDisconnectedState() {
        // Set up disconnected state
        session.close();

        // Send items while disconnected
        session.onNext(List.of(SAMPLE_ROUND_HEADER));

        // Verify state remains DISCONNECTED
        assertEquals(BlockStreamProducerSession.BlockState.DISCONNECTED, session.currentBlockState());
    }

    /**
     * Tests handling of complete block processing sequence.
     * Verifies that the session correctly processes a complete block from start to finish.
     */
    @Test
    @DisplayName("Should handle complete block processing sequence")
    void testCompleteBlockProcessing() {
        // Start block with header
        session.onNext(List.of(SAMPLE_BLOCK_HEADER));
        assertEquals(BlockStreamProducerSession.BlockState.NEW, session.currentBlockState());

        // Switch to primary
        session.switchToPrimary();

        // Add transaction results
        session.onNext(List.of(SAMPLE_ROUND_HEADER));
        assertEquals(BlockStreamProducerSession.BlockState.PRIMARY, session.currentBlockState());

        // End block with proof
        session.onNext(List.of(SAMPLE_BLOCK_PROOF));

        // Verify final state
        assertEquals(BlockStreamProducerSession.BlockState.NEW, session.currentBlockState());
    }

    /**
     * Tests the session's response to block resend requests.
     * Verifies that the session correctly handles requests to resend blocks.
     */
    @Test
    @DisplayName("Should handle block resend requests")
    void testRequestResend() {
        // Request resend of block 100
        session.requestResend(100L);

        // Verify state change
        assertEquals(BlockStreamProducerSession.BlockState.WAITING_FOR_RESEND, session.currentBlockState());
        assertEquals(100L, session.currentBlockNumber());
        assertNotNull(lastResponse);
        assertEquals(
                PublishStreamResponse.ResponseOneOfType.RESEND_BLOCK,
                lastResponse.response().kind());
    }

    /**
     * Tests session cleanup and disconnection.
     * Verifies that the session properly cleans up resources when closed.
     */
    @Test
    @DisplayName("Should clean up resources on close")
    void testClose() {
        // Create a mock subscription
        Flow.Subscription mockSubscription = new Flow.Subscription() {
            @Override
            public void request(long n) {}

            @Override
            public void cancel() {}
        };

        // Set up subscription
        session.onSubscribe(mockSubscription);

        // Close the session
        session.close();

        // Verify state change
        assertEquals(BlockStreamProducerSession.BlockState.DISCONNECTED, session.currentBlockState());
        assertNotNull(lastResponse);
        assertEquals(
                PublishStreamResponse.ResponseOneOfType.END_STREAM,
                lastResponse.response().kind());
    }

    /**
     * Tests handling of block persistence notifications.
     * Verifies that the session correctly sends acknowledgments when blocks are persisted.
     */
    @Test
    @DisplayName("Should handle block persistence notifications")
    @Disabled // TODO this needs more work as logic is more complex now
    void testSendBlockPersisted() {
        // Send block persisted notification
        session.handlePersisted(new PersistedNotification(100L, 100L, 1));

        assertNotNull(lastResponse);
        assertEquals(
                PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT,
                lastResponse.response().kind());
    }

    /**
     * Tests error handling in the session.
     * Verifies that the session properly handles errors and updates its state accordingly.
     */
    @Test
    @DisplayName("Should handle errors gracefully")
    void testErrorHandling() {
        // Simulate an error
        session.onError(new RuntimeException("Test error"));

        assertEquals(BlockStreamProducerSession.BlockState.DISCONNECTED, session.currentBlockState());
        assertNotNull(lastResponse);
        assertEquals(
                PublishStreamResponse.ResponseOneOfType.END_STREAM,
                lastResponse.response().kind());
    }

    /**
     * Tests handling of stream completion.
     * Verifies that the session properly handles stream completion events.
     */
    @Test
    @DisplayName("Should handle stream completion")
    void testStreamCompletion() {
        // Simulate stream completion
        session.onComplete();

        // Verify state change
        assertEquals(BlockStreamProducerSession.BlockState.DISCONNECTED, session.currentBlockState());
    }

    private class ResponsePipeline implements Pipeline<PublishStreamResponse> {
        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            throw new UnsupportedOperationException("This pipeline does not support subscriptions.");
        }

        @Override
        public void clientEndStreamReceived() {
            Pipeline.super.clientEndStreamReceived();
        }

        @Override
        public void onNext(PublishStreamResponse item) throws RuntimeException {
            BlockStreamProducerSessionTest.this.lastResponse = item;
        }

        @Override
        public void onError(Throwable throwable) {
            // Not used
        }

        @Override
        public void onComplete() {
            // Not used
        }
    }
}
