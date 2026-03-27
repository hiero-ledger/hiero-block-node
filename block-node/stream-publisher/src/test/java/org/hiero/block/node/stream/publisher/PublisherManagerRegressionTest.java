// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hiero.block.node.stream.publisher.fixtures.PublishApiUtility.endThisBlock;
import static org.hiero.block.node.stream.publisher.fixtures.PublishApiUtility.sendHeaderOnly;

import com.swirlds.config.api.Configuration;
import com.swirlds.metrics.api.Metrics;
import java.util.concurrent.LinkedBlockingQueue;
import org.hiero.block.api.BlockNodeVersions;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.PublishStreamResponse.EndOfStream.Code;
import org.hiero.block.api.PublishStreamResponse.ResponseOneOfType;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.pipeline.TestResponsePipeline;
import org.hiero.block.node.app.fixtures.plugintest.SimpleBlockRangeSet;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.node.spi.threading.ThreadPoolManager;
import org.hiero.block.node.stream.publisher.LiveStreamPublisherManager.MetricsHolder;
import org.hiero.block.node.stream.publisher.StreamPublisherManager.BlockAction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Regression tests for [LiveStreamPublisherManager]: backfill gap
/// advancement and stall detection when the ACCEPT winner goes silent.
@DisplayName("PublisherManager Regression Tests")
class PublisherManagerRegressionTest {

    private SimpleInMemoryHistoricalBlockFacility historicalBlockFacility;
    private LiveStreamPublisherManager toTest;
    private MetricsHolder managerMetrics;
    private PublisherHandler.MetricsHolder sharedHandlerMetrics;

    private TestResponsePipeline<PublishStreamResponse> responsePipeline;
    private PublisherHandler publisherHandler;
    private long publisherHandlerId;

    private TestResponsePipeline<PublishStreamResponse> responsePipeline2;
    private PublisherHandler publisherHandler2;

    @BeforeEach
    void setup() {
        historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
        final TestThreadPoolManager<BlockingExecutor, ScheduledBlockingExecutor> threadPoolManager =
                new TestThreadPoolManager<>(
                        new BlockingExecutor(new LinkedBlockingQueue<>()),
                        new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        final TestBlockMessagingFacility messagingFacility = new TestBlockMessagingFacility();
        final BlockNodeContext context = generateContext(historicalBlockFacility, threadPoolManager, messagingFacility);
        historicalBlockFacility.init(context, null);

        managerMetrics = MetricsHolder.createMetrics(TestUtils.createMetrics());
        toTest = new LiveStreamPublisherManager(context, managerMetrics);
        context.blockMessaging()
                .registerBlockNotificationHandler(toTest, false, LiveStreamPublisherManager.class.getSimpleName());

        sharedHandlerMetrics = PublisherHandler.MetricsHolder.createMetrics(TestUtils.createMetrics());
        responsePipeline = new TestResponsePipeline<>();
        publisherHandler = toTest.addHandler(responsePipeline, sharedHandlerMetrics);
        publisherHandlerId = 0L;

        responsePipeline2 = new TestResponsePipeline<>();
        publisherHandler2 = toTest.addHandler(responsePipeline2, sharedHandlerMetrics);
    }

    /// Verifies that when backfill persists blocks that advance past the
    /// current {@code nextUnstreamedBlockNumber}, the manager updates its
    /// tracking so that the next publisher block is accepted rather than
    /// rejected with SEND_BEHIND.
    @Test
    @DisplayName("handlePersisted() advances nextUnstreamedBlockNumber when backfill fills a gap")
    void testBackfillAdvancesNextUnstreamedBlockNumber() {
        // Initial state: blocks 0-2 persisted, next expected is 3
        final long initialLastPersisted = 2L;
        final SimpleBlockRangeSet availableBlocks = new SimpleBlockRangeSet();
        availableBlocks.add(0L, initialLastPersisted);
        historicalBlockFacility.setTemporaryAvailableBlocks(availableBlocks);
        toTest.handlePersisted(new PersistedNotification(initialLastPersisted, true, 0, BlockSource.UNKNOWN));

        // Backfill fills blocks 3-5
        final long backfilledBlock = 5L;
        toTest.handlePersisted(new PersistedNotification(backfilledBlock, true, 0, BlockSource.BACKFILL));

        // Publisher sending block 6 should be ACCEPTED (not SEND_BEHIND)
        final BlockAction action = toTest.getActionForBlock(6L, null, publisherHandlerId);
        assertThat(action)
                .describedAs("After backfill to block 5, block 6 should be ACCEPT not SEND_BEHIND")
                .isEqualTo(BlockAction.ACCEPT);
    }

    /// Verifies that backfill can recover a pipeline stalled by a silent
    /// ACCEPT winner. Handler 1 wins ACCEPT for block 0, sends only the
    /// header, then goes silent. Handler 2 receives SKIP for block 0.
    /// Backfill then persists blocks 0 through 2, advancing the manager
    /// past the stalled block. After that, handler 2 sends block 3 and
    /// must receive ACCEPT — the pipeline is unstuck.
    ///
    /// This works because the {@code isBeforeEarliestActiveBlock} guard
    /// in {@code handlePersisted()} was disabled so that backfill
    /// notifications can advance the manager even when a stalled handler
    /// holds an incomplete queue entry for the persisted block.
    @Test
    @DisplayName("backfill persisting the stalled block and beyond unblocks a frozen pipeline")
    void testBackfillUnblocksStalledAcceptWinner() {
        final long stalledBlock = 0L;

        // Handler 1 wins ACCEPT for block 0, sends only the header, then goes silent.
        sendHeaderOnly(publisherHandler, stalledBlock);

        // Handler 2 sends block 0 — receives SKIP because handler 1 holds ACCEPT.
        final PublishStreamRequestUnparsed fullBlock0 = PublishStreamRequestUnparsed.newBuilder()
                .blockItems(
                        TestBlockBuilder.generateBlockWithNumber(stalledBlock).asItemSetUnparsed())
                .build();
        publisherHandler2.onNext(fullBlock0);
        assertThat(responsePipeline2.getOnNextCalls())
                .as("handler 2 must receive SKIP for block 0")
                .hasSize(1)
                .first()
                .returns(ResponseOneOfType.SKIP_BLOCK, response -> response.response()
                        .kind());

        // Backfill persists blocks 0 through 2, covering and advancing past
        // the stalled block.
        final long lastBackfilledBlock = 2L;
        final SimpleBlockRangeSet availableBlocks = new SimpleBlockRangeSet();
        availableBlocks.add(stalledBlock, lastBackfilledBlock);
        historicalBlockFacility.setTemporaryAvailableBlocks(availableBlocks);
        toTest.handlePersisted(new PersistedNotification(stalledBlock, true, 0, BlockSource.BACKFILL));
        toTest.handlePersisted(new PersistedNotification(1L, true, 0, BlockSource.BACKFILL));
        toTest.handlePersisted(new PersistedNotification(lastBackfilledBlock, true, 0, BlockSource.BACKFILL));

        // Handler 2 sends block 3 — must be ACCEPTED now that backfill
        // advanced the manager past the stalled block.
        final long nextLiveBlock = lastBackfilledBlock + 1;
        final BlockAction action = toTest.getActionForBlock(nextLiveBlock, null, publisherHandlerId);
        assertThat(action)
                .describedAs(
                        "After backfill persisted blocks 0-2, block %d should be ACCEPT not SEND_BEHIND", nextLiveBlock)
                .isEqualTo(BlockAction.ACCEPT);
    }

    /// Verifies the 2-block stall detection and recovery mechanism:
    /// when the ACCEPT winner goes silent mid-block and another publisher
    /// completes 2 blocks beyond the stalled one, the manager must detect
    /// the stall, disconnect the silent handler with
    /// {@code EndStream(TIMEOUT)}, and send {@code ResendBlock} to the
    /// remaining publisher so it can take over.
    ///
    /// This test is expected to fail until @todo(#1841) is implemented.
    @Test
    @DisplayName("timeout detection and block resend triggered after 2-block stall — @todo(#1841)")
    @Disabled("active-queue guard removed to allow backfill recovery — re-enable with @todo(#1841)")
    void testTimeoutDetectionAndBlockResendTriggeredForSkipPublishers() {
        final long stalledBlock = 0L;
        final long block1 = 1L;
        final long block2 = 2L;

        // Handler 1 wins ACCEPT for block 0, sends only the header, then goes silent.
        sendHeaderOnly(publisherHandler, stalledBlock);

        // Handler 2 sends block 0 — receives SKIP because handler 1 holds ACCEPT.
        final PublishStreamRequestUnparsed fullBlock0 = PublishStreamRequestUnparsed.newBuilder()
                .blockItems(
                        TestBlockBuilder.generateBlockWithNumber(stalledBlock).asItemSetUnparsed())
                .build();
        publisherHandler2.onNext(fullBlock0);
        assertThat(responsePipeline2.getOnNextCalls())
                .as("handler 2 must receive SKIP for block 0")
                .hasSize(1)
                .first()
                .returns(ResponseOneOfType.SKIP_BLOCK, response -> response.response()
                        .kind());

        // Handler 2 completes block 1.
        final PublishStreamRequestUnparsed fullBlock1 = PublishStreamRequestUnparsed.newBuilder()
                .blockItems(TestBlockBuilder.generateBlockWithNumber(block1).asItemSetUnparsed())
                .build();
        publisherHandler2.onNext(fullBlock1);
        endThisBlock(publisherHandler2, block1);

        // Handler 2 completes block 2.
        // Two complete blocks now exist beyond the stalled block.
        final PublishStreamRequestUnparsed fullBlock2 = PublishStreamRequestUnparsed.newBuilder()
                .blockItems(TestBlockBuilder.generateBlockWithNumber(block2).asItemSetUnparsed())
                .build();
        publisherHandler2.onNext(fullBlock2);
        endThisBlock(publisherHandler2, block2);

        // The manager must detect the stall and recover.

        // Assert: stalled handler 1 received EndStream(TIMEOUT) and was closed.
        assertThat(responsePipeline.getOnNextCalls())
                .as("stalled ACCEPT handler must receive EndStream(TIMEOUT)")
                .anySatisfy(response -> {
                    assertThat(response.response().kind()).isEqualTo(ResponseOneOfType.END_STREAM);
                    assertThat(response.endStream().status()).isEqualTo(Code.TIMEOUT);
                });
        assertThat(responsePipeline.getOnCompleteCalls().get())
                .as("stalled ACCEPT handler must be closed after timeout")
                .isEqualTo(1);

        // Assert: handler 2 received ResendBlock(0) so it can take over.
        assertThat(responsePipeline2.getOnNextCalls())
                .as("handler 2 must receive ResendBlock for the stalled block")
                .anySatisfy(response -> {
                    assertThat(response.response().kind()).isEqualTo(ResponseOneOfType.RESEND_BLOCK);
                    assertThat(response.resendBlock().blockNumber()).isEqualTo(stalledBlock);
                });
    }

    @SuppressWarnings("all")
    private BlockNodeContext generateContext(
            final HistoricalBlockFacility historicalBlockFacility,
            final ThreadPoolManager threadPoolManager,
            final BlockMessagingFacility blockMessagingFacility) {
        final Configuration configuration = TestUtils.createTestConfiguration()
                .withConfigDataType(PublisherConfig.class)
                .build();
        final Metrics metrics = TestUtils.createMetrics();
        final HealthFacility serverHealth = null;
        final ServiceLoaderFunction serviceLoader = null;
        return new BlockNodeContext(
                configuration,
                metrics,
                serverHealth,
                blockMessagingFacility,
                historicalBlockFacility,
                serviceLoader,
                threadPoolManager,
                BlockNodeVersions.DEFAULT);
    }
}
