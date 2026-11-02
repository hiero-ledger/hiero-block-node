// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.concurrent.LinkedBlockingQueue;
import org.hiero.block.api.PublishStreamRequest.AcknowledgeOnly;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.PublishStreamResponse.BlockAcknowledgement;
import org.hiero.block.api.PublishStreamResponse.ResponseOneOfType;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.pipeline.TestResponsePipeline;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.stream.publisher.PublisherHandler.MetricsHolder;
import org.hiero.block.node.stream.publisher.StreamPublisherManager.BlockAction;
import org.hiero.block.node.stream.publisher.fixtures.TestStreamPublisherManager;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Tests for the AcknowledgeOnly request-handling behavior of [PublisherHandler].
///
/// Covers the active and passive handler gauges, immediate acknowledgement for already-persisted block numbers,
/// deferred acknowledgement when the Block-Node is behind, and mid-block cancellation
/// when a Publisher switches from streaming to AcknowledgeOnly.
@DisplayName("PublisherHandler AcknowledgeOnly Tests")
class PublisherHandlerAcknowledgeOnlyTest {

    private static final long HANDLER_ID = 1L;

    private TestResponsePipeline<PublishStreamResponse> replies;
    private MetricsHolder metrics;
    private TestStreamPublisherManager manager;
    private PublisherHandler handler;

    @BeforeEach
    void setup() {
        replies = new TestResponsePipeline<>();
        metrics = createMetrics();
        manager = new TestStreamPublisherManager(
                new TestBlockMessagingFacility(), new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        handler = new PublisherHandler(HANDLER_ID, replies, metrics, manager, null);
        manager.addHandler(handler);
    }

    @Nested
    @DisplayName("Mode gauge tests")
    class ModeGaugeTests {

        @Test
        @DisplayName("A new handler is counted as active")
        void newHandlerIsActive() {
            assertCounts(1L, 0L);
        }

        @Test
        @DisplayName("Receiving AcknowledgeOnly moves the handler from the active to the passive gauge")
        void acknowledgeOnlyFlipsToPassive() {
            handler.onNext(ackOnlyRequest(5L));

            assertCounts(0L, 1L);
        }

        @Test
        @DisplayName("A repeated AcknowledgeOnly does not change the gauges")
        void repeatedAcknowledgeOnlyIsStable() {
            handler.onNext(ackOnlyRequest(5L));
            handler.onNext(ackOnlyRequest(6L));

            assertCounts(0L, 1L);
        }

        @Test
        @DisplayName("Receiving BlockItems after AcknowledgeOnly moves the handler back to the active gauge")
        void blockItemsFlipsBackToActive() {
            handler.onNext(ackOnlyRequest(5L));
            assertCounts(0L, 1L);

            manager.setBlockActionForBlock(BlockAction.ACCEPT);
            handler.onNext(blockItemsRequest(0L));

            assertCounts(1L, 0L);
        }

        @Test
        @DisplayName("Closing an active handler removes it from the active gauge")
        void closeFromActive() {
            handler.onComplete();

            assertCounts(0L, 0L);
        }

        @Test
        @DisplayName("Closing a passive handler removes it from the passive gauge")
        void closeFromPassive() {
            handler.onNext(ackOnlyRequest(5L));

            handler.onComplete();

            assertCounts(0L, 0L);
        }

        @Test
        @DisplayName("Closing a handler more than once does not decrement the gauges again")
        void closeIsExactlyOnce() {
            final PublisherHandler other = new PublisherHandler(HANDLER_ID + 1, replies, metrics, manager, null);
            assertCounts(2L, 0L);

            handler.onComplete();
            handler.onComplete();
            handler.onError(new RuntimeException("closed twice"));

            assertCounts(1L, 0L);
            other.onComplete();
            assertCounts(0L, 0L);
        }

        @Test
        @DisplayName("A request received after close does not put the handler back in a gauge")
        void requestAfterCloseIsNotCounted() {
            handler.onComplete();

            handler.onNext(ackOnlyRequest(5L));

            assertCounts(0L, 0L);
        }

        private void assertCounts(final long active, final long passive) {
            assertThat(metrics.activeHandlers().get()).isEqualTo(active);
            assertThat(metrics.passiveHandlers().get()).isEqualTo(passive);
        }
    }

    @Nested
    @DisplayName("Immediate acknowledgement tests")
    class ImmediateAcknowledgementTests {

        @Test
        @DisplayName("AcknowledgeOnly below the latest acked block triggers immediate ack at latest")
        void immediateAckWhenBelowLatest() {
            manager.setLatestBlockNumber(10L);

            handler.onNext(ackOnlyRequest(5L));

            assertThat(replies.getOnNextCalls()).hasSize(1);
            final PublishStreamResponse response = replies.getOnNextCalls().get(0);
            assertThat(response.response().kind()).isEqualTo(ResponseOneOfType.ACKNOWLEDGEMENT);
            final BlockAcknowledgement ack = response.acknowledgement();
            assertThat(ack).isNotNull();
            assertThat(ack.blockNumber()).isEqualTo(10L);
        }

        @Test
        @DisplayName("AcknowledgeOnly equal to the latest acked block triggers immediate ack")
        void immediateAckWhenEqualToLatest() {
            manager.setLatestBlockNumber(7L);

            handler.onNext(ackOnlyRequest(7L));

            assertThat(replies.getOnNextCalls()).hasSize(1);
            assertThat(replies.getOnNextCalls().get(0).acknowledgement().blockNumber())
                    .isEqualTo(7L);
        }

        @Test
        @DisplayName("AcknowledgeOnly ahead of the latest acked block does NOT trigger an ack")
        void noAckWhenAheadOfLatest() {
            manager.setLatestBlockNumber(3L);

            handler.onNext(ackOnlyRequest(8L));

            assertThat(replies.getOnNextCalls()).isEmpty();
            assertThat(metrics.passiveHandlers().get()).isEqualTo(1L);
        }

        @Test
        @DisplayName("Immediate acknowledgement carries the retained root hash of the latest acked block")
        void ackCarriesRetainedRootHash() {
            manager.setLatestBlockNumber(10L);
            final Bytes expectedHash = Bytes.wrap(new byte[] {1, 2, 3, 4});
            manager.setLatestAckedBlockRootHash(10L, expectedHash);

            handler.onNext(ackOnlyRequest(5L));

            assertThat(replies.getOnNextCalls()).hasSize(1);
            final BlockAcknowledgement ack = replies.getOnNextCalls().get(0).acknowledgement();
            assertThat(ack.blockNumber()).isEqualTo(10L);
            assertThat(ack.blockRootHash()).isEqualTo(expectedHash);
        }

        @Test
        @DisplayName("Immediate acknowledgement carries an empty root hash when none is retained for the latest block")
        void ackCarriesEmptyRootHashWhenNoneRetained() {
            manager.setLatestBlockNumber(10L);
            manager.setLatestAckedBlockRootHash(9L, Bytes.wrap(new byte[] {9}));

            handler.onNext(ackOnlyRequest(10L));

            assertThat(replies.getOnNextCalls()).hasSize(1);
            final BlockAcknowledgement ack = replies.getOnNextCalls().get(0).acknowledgement();
            assertThat(ack.blockRootHash()).isEqualTo(Bytes.EMPTY);
        }
    }

    @Nested
    @DisplayName("Mid-block cancellation tests")
    class MidBlockCancellationTests {

        @Test
        @DisplayName("AcknowledgeOnly mid-block cancels the partial block on the manager and resets handler state")
        void acknowledgeOnlyCancelsMidBlock() {
            manager.setBlockActionForBlock(BlockAction.ACCEPT);
            // Start streaming block 7 (handler enters mid-block state and registers the queue).
            handler.onNext(blockItemsRequest(7L));
            assertThat(manager.getQueueForBlock(7L)).isNotNull();
            assertThat(manager.getBlocksEndedMidBlock()).doesNotContain(7L);

            handler.onNext(ackOnlyRequest(7L));

            assertThat(manager.getBlocksEndedMidBlock()).contains(7L);
            assertThat(metrics.passiveHandlers().get()).isEqualTo(1L);
            // A subsequent BlockItems request with a new header must be accepted as a fresh block
            // (i.e. the handler is no longer mid-block on 7).
            handler.onNext(blockItemsRequest(8L));
            assertThat(metrics.activeHandlers().get()).isEqualTo(1L);
        }

        @Test
        @DisplayName("AcknowledgeOnly with no in-progress block does not call blockIsEnding")
        void acknowledgeOnlyNoMidBlockDoesNotCancel() {
            handler.onNext(ackOnlyRequest(5L));

            assertThat(manager.getBlocksEndedMidBlock()).isEmpty();
            assertThat(metrics.passiveHandlers().get()).isEqualTo(1L);
        }
    }

    private PublishStreamRequestUnparsed ackOnlyRequest(final long blockNumber) {
        return PublishStreamRequestUnparsed.newBuilder()
                .acknowledgeOnly(
                        AcknowledgeOnly.newBuilder().blockNumber(blockNumber).build())
                .build();
    }

    private PublishStreamRequestUnparsed blockItemsRequest(final long blockNumber) {
        final TestBlock block = TestBlockBuilder.generateBlockWithNumber(blockNumber);
        final BlockItemSetUnparsed set = block.asItemSetUnparsed();
        return PublishStreamRequestUnparsed.newBuilder().blockItems(set).build();
    }

    private MetricsHolder createMetrics() {
        return MetricsHolder.createMetrics(MetricRegistry.builder().build());
    }
}
