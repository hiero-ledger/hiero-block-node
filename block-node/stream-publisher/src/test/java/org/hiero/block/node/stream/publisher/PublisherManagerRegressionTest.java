// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hiero.block.node.stream.publisher.PublishApiUtility.endThisBlock;
import static org.hiero.block.node.stream.publisher.PublishApiUtility.sendHeaderOnly;

import com.swirlds.config.api.Configuration;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BooleanSupplier;
import org.hiero.block.api.PublishStreamRequest.EndStream;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.PublishStreamResponse.EndOfStream.Code;
import org.hiero.block.api.PublishStreamResponse.ResponseOneOfType;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.pipeline.TestResponsePipeline;
import org.hiero.block.node.app.fixtures.plugintest.SimpleBlockRangeSet;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestApplicationStateFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.node.spi.threading.ThreadPoolManager;
import org.hiero.block.node.stream.publisher.LiveStreamPublisherManager.MetricsHolder;
import org.hiero.block.node.stream.publisher.StreamPublisherManager.BlockAction;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Regression tests for [LiveStreamPublisherManager]: backfill gap
/// advancement and stall detection when the ACCEPT winner goes silent.
@DisplayName("PublisherManager Regression Tests")
class PublisherManagerRegressionTest {

    private SimpleInMemoryHistoricalBlockFacility historicalBlockFacility;
    private TestThreadPoolManager<BlockingExecutor, ScheduledBlockingExecutor> threadPoolManager;
    private TestBlockMessagingFacility messagingFacility;
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
        threadPoolManager = new TestThreadPoolManager<>(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        messagingFacility = new TestBlockMessagingFacility();
        final BlockNodeContext context = generateContext(historicalBlockFacility, threadPoolManager, messagingFacility);
        historicalBlockFacility.init(context, null);

        managerMetrics = MetricsHolder.createMetrics(TestUtils.createMetrics());
        toTest = new LiveStreamPublisherManager(context, managerMetrics, List.of());
        context.blockMessaging()
                .registerBlockNotificationHandler(toTest, false, LiveStreamPublisherManager.class.getSimpleName());

        sharedHandlerMetrics = PublisherHandler.MetricsHolder.createMetrics(TestUtils.createMetrics());
        responsePipeline = new TestResponsePipeline<>();
        publisherHandler = toTest.addHandler(responsePipeline, sharedHandlerMetrics, null);
        publisherHandlerId = 0L;

        responsePipeline2 = new TestResponsePipeline<>();
        publisherHandler2 = toTest.addHandler(responsePipeline2, sharedHandlerMetrics, "");
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
        // the stalled block. This won't free the stall, however, because
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

    /// Reproduces the previewnet 0.31.0-rc1 forwarder deadlock.
    ///
    /// A handler goes silent mid-block N. Backfill persists block N,
    /// advancing {@code lastPersistedBlockNumber} to N. Handler 2 then
    /// sends block N+1. The forwarder must deliver block N+1 to messaging.
    ///
    /// The bug: {@code clearObsoleteQueueItems(N)} uses
    /// {@code headMap(N)} (strictly less than), so block N's incomplete
    /// queue is never removed. The forwarder's
    /// {@code determineCurrentBlockNumber()} picks it via
    /// {@code firstEntry()} and gets stuck forever — block N+1 never
    /// reaches messaging even though the manager returned ACCEPT.
    @Test
    @DisplayName("forwarder advances past stalled block after backfill persists it — previewnet 0.31.0-rc1")
    void testForwarderAdvancesPastStalledBlockAfterBackfill() {
        // Blocks 0-4 already persisted. Handler 1 stalls on block 5.
        final long lastPreStallBlock = 4L;
        final long stalledBlock = 5L;

        // Establish initial persisted state: blocks 0-4 are already known.
        final SimpleBlockRangeSet initialBlocks = new SimpleBlockRangeSet();
        initialBlocks.add(0L, lastPreStallBlock);
        historicalBlockFacility.setTemporaryAvailableBlocks(initialBlocks);
        toTest.handlePersisted(new PersistedNotification(lastPreStallBlock, true, 0, BlockSource.PUBLISHER));

        // Handler 1 wins ACCEPT for block 5, sends only the header, then goes silent.
        sendHeaderOnly(publisherHandler, stalledBlock);

        // Handler 2 sends block 5 — receives SKIP because handler 1 holds ACCEPT.
        responsePipeline2.clear();
        final PublishStreamRequestUnparsed fullStalledBlock = PublishStreamRequestUnparsed.newBuilder()
                .blockItems(
                        TestBlockBuilder.generateBlockWithNumber(stalledBlock).asItemSetUnparsed())
                .build();
        publisherHandler2.onNext(fullStalledBlock);
        assertThat(responsePipeline2.getOnNextCalls())
                .as("handler 2 must receive SKIP for block %d", stalledBlock)
                .hasSize(1)
                .first()
                .returns(ResponseOneOfType.SKIP_BLOCK, response -> response.response()
                        .kind());

        // Handler 2 sends a complete block 6.
        responsePipeline2.clear();
        final long nextLiveBlock = stalledBlock + 1;
        final TestBlock nextBlock = TestBlockBuilder.generateBlockWithNumber(nextLiveBlock);
        publisherHandler2.onNext(nextBlock.asPublishStreamRequestUnparsed());
        endThisBlock(publisherHandler2, nextLiveBlock);

        // Backfill persists block 5, This must not advance last persisted (because block 5 is actively streaming).
        // clearObsoleteQueueItems(5) uses headMap(5) which does NOT
        // include block 5 — the stalled queue stays in the map.
        // Except that persisted block == stalled block, so the stalled block is abandoned.
        // This clears the blockage and block 6 will now proceed.
        final SimpleBlockRangeSet backfilledBlocks = new SimpleBlockRangeSet();
        backfilledBlocks.add(0L, stalledBlock);
        historicalBlockFacility.setTemporaryAvailableBlocks(backfilledBlocks);
        toTest.handlePersisted(new PersistedNotification(stalledBlock, true, 0, BlockSource.BACKFILL));

        // Run the forwarder. If the stalled block 5's queue is still
        // in queueByBlockMap, determineCurrentBlockNumber() returns 5
        // and the forwarder never reaches block 6.
        threadPoolManager.executor().executeAsync(1_000L, false);

        assertThat(messagingFacility.getSentBlockItems())
                .as(
                        "block %d must be forwarded to messaging after backfill unblocked the pipeline via stall detection",
                        nextLiveBlock)
                .anyMatch(items -> items.blockNumber() == nextLiveBlock);
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

    /// Reproduces the production stale-resend bug observed on release 0.33.0-rc3.
    ///
    /// In production, a TOCTOU race between {@link LiveStreamPublisherManager#blockIsEnding(long, long)}
    /// and a concurrent BACKFILL-sourced {@link LiveStreamPublisherManager#handlePersisted} call
    /// leaves a block number in {@code blocksToResend} after {@code lastPersistedBlockNumber}
    /// has already advanced past it. From that point on, every fresh publisher's first
    /// {@code endOfBlock} returns {@code RESEND(<stale block>)} and the publisher disconnects
    /// with EndStream(TOO_FAR_BEHIND).
    ///
    /// We can't easily drive the race deterministically in a unit test, but the fix —
    /// pruning stale entries during {@code handlePersisted} — has an observable invariant
    /// that we can test with real publish-API operations:
    ///
    /// 1. A publisher sends only the header for block N then issues EndStream(RESET). The
    ///    real {@code blockIsEnding(N)} path adds N to {@code blocksToResend}.
    /// 2. Backfill persists block {@code N + buffer + delta} (well past the prune buffer).
    /// 3. With the fix, {@code handlePersisted} prunes the now-stale entry for N before
    ///    {@code correctForResendAndStreaming} can clamp the ack. {@code lastPersistedBlockNumber}
    ///    advances and a fresh publisher's header for N is treated as
    ///    {@code END_DUPLICATE} — not {@code ACCEPT} as if N were still expected.
    ///
    /// Without the fix the entry is never pruned, {@code lastPersistedBlockNumber} is
    /// clamped at {@code N - 1}, and {@code getActionForBlock(N, ...)} pulls N out of
    /// {@code blocksToResend} and returns {@code ACCEPT} — exactly the production symptom
    /// where the BN keeps demanding the stale block from any publisher that connects.
    ///
    /// Note: the seed step uses only the publish API ({@code sendHeaderOnly} +
    /// {@code EndStream}), so the entry is added by the same production code path
    /// ({@code PublisherHandler.handleEndStream} → {@code LiveStreamPublisherManager.blockIsEnding})
    /// the bug actually exercises — no internal state injection.
    @Test
    @DisplayName("handlePersisted must prune resend entries beyond the prune buffer")
    void testStaleResendEntryPrunedWhenPersistenceMovesPastBuffer() {
        // 1. Persist blocks up to 4 so the manager will accept a header for block 5
        //    (nextUnstreamedBlockNumber == 5). Without this, sendHeaderOnly(5) returns
        //    SEND_BEHIND and the handler never enters mid-block state.
        final long lastPersistedBeforeStall = 4L;
        final SimpleBlockRangeSet preStallBlocks = new SimpleBlockRangeSet();
        preStallBlocks.add(0L, lastPersistedBeforeStall);
        historicalBlockFacility.setTemporaryAvailableBlocks(preStallBlocks);
        toTest.handlePersisted(new PersistedNotification(lastPersistedBeforeStall, true, 0, BlockSource.UNKNOWN));

        // 2. Publisher A sends a header for block 5 then EndStream(RESET) mid-block.
        //    handleEndStream invokes publisherManager.blockIsEnding(5, …), which adds 5
        //    to blocksToResend through the same code path the production bug exercises.
        //    No internal state is injected — we drive the same publish API that production uses.
        final long staleResendBlock = 5L;
        sendHeaderOnly(publisherHandler, staleResendBlock);

        final EndStream endStream = EndStream.newBuilder()
                .endCode(EndStream.Code.RESET)
                .earliestBlockNumber(0L)
                .latestBlockNumber(staleResendBlock)
                .build();
        publisherHandler.onNext(
                PublishStreamRequestUnparsed.newBuilder().endStream(endStream).build());
        // !! IMPORTANT !!
        //    This removes `publisherHandler`, so remainder of this test _must_ use
        //    `publisherHandler2` or the results will be wrong.

        // 3. Backfill persists a block far past the configured prune buffer (default 100).
        //    handlePersisted's prune step must drop blocksToResend entries <= 200 - 100 = 100,
        //    which includes block 5.
        final long backfilledBlock = 200L;
        final SimpleBlockRangeSet availableBlocks = new SimpleBlockRangeSet();
        availableBlocks.add(0L, backfilledBlock);
        historicalBlockFacility.setTemporaryAvailableBlocks(availableBlocks);
        toTest.handlePersisted(new PersistedNotification(backfilledBlock, true, 0, BlockSource.BACKFILL));

        // 4. A fresh publisher header for the previously-stale block must be treated as
        //    END_DUPLICATE — not ACCEPT pulled out of blocksToResend, which would prove
        //    the entry was never pruned. (END_DUPLICATE comes from blockNumber <= lastPersisted,
        //    which only happens if both prune and ack-advancement succeeded.)
        final BlockAction action = toTest.getActionForBlock(staleResendBlock, null, publisherHandler2.getId());
        assertThat(action)
                .describedAs(
                        "block %d is already persisted (lastPersisted should be %d). The handler must see "
                                + "END_DUPLICATE — not ACCEPT — which would only happen if the stale entry "
                                + "had been pruned from blocksToResend by handlePersisted",
                        staleResendBlock, backfilledBlock)
                .isEqualTo(BlockAction.END_DUPLICATE);
    }

    // ---------------------------------------------------------------------------------------------
    // Stale block proof after a publisher drops mid-block.
    //
    // The forwarder polls a block's proof out of the queue and holds it in blockProofs until the
    // publisher's end-of-block arrives. If the publisher drops before that, the proof belongs to an
    // abandoned attempt. It must not survive into the resent attempt, where it hides the new queue
    // from the forwarder and is sent on its own as the end of a block that was never started.
    // ---------------------------------------------------------------------------------------------

    /// Reads a private collection field of the manager under test.
    @SuppressWarnings("unchecked")
    private <T> T managerField(final String name) throws Exception {
        final Field field = LiveStreamPublisherManager.class.getDeclaredField(name);
        field.setAccessible(true);
        return (T) field.get(toTest);
    }

    /// Lets the forwarder run once over whatever is queued.
    private void runForwarder() {
        // Make sure a forwarder task is queued, then run it until it has drained the queues. If a
        // forwarder from an earlier call is still running it picks the new work up by itself.
        toTest.checkLogAndRestartForwarderTask();
        if (!threadPoolManager.executor().getQueue().isEmpty()) {
            threadPoolManager.executor().executeAsync(1_000L, false);
        }
    }

    /// Waits briefly for the forwarder, running on another thread, to produce the awaited messages.
    private static void awaitForwarded(final BooleanSupplier condition) {
        final long deadline = System.currentTimeMillis() + 3_000L;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(25L);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /// The forwarded messages for a block, in the order they were sent to messaging.
    private List<BlockItems> forwardedFor(final long blockNumber) {
        return messagingFacility.getSentBlockItems().stream()
                .filter(items -> items.blockNumber() == blockNumber)
                .toList();
    }

    /// A publisher streams a whole block and the forwarder collects its proof, then the publisher
    /// drops before sending end-of-block. A second publisher is then asked to resend the block.
    /// Returns once the first attempt has been abandoned and the forwarder has run over it.
    private TestBlock streamWholeBlockThenDrop(final long blockNumber) {
        final TestBlock block = TestBlockBuilder.generateBlockWithNumber(blockNumber);
        publisherHandler.onNext(block.asPublishStreamRequestUnparsed());
        runForwarder();
        assertThat(forwardedFor(blockNumber))
                .as("first attempt: header and body are forwarded, the proof is held back")
                .isNotEmpty()
                .noneMatch(BlockItems::isEndOfBlock)
                .first()
                .returns(true, BlockItems::isStartOfNewBlock);
        // The publisher's connection is lost before it sends end-of-block.
        toTest.blockIsEnding(blockNumber, publisherHandlerId);
        return block;
    }

    /// Dropping a publisher mid-block must discard the proof and end marker collected for its attempt.
    @Test
    @DisplayName("blockIsEnding() discards the proof collected for the abandoned attempt")
    void testBlockIsEndingDiscardsCollectedProof() throws Exception {
        streamWholeBlockThenDrop(0L);

        final Map<Long, BlockItemUnparsed> blockProofs = managerField("blockProofs");
        final Set<Long> endBlocksReceived = managerField("endBlocksReceived");
        assertThat(blockProofs)
                .as("stale proof must not survive the dropped attempt")
                .doesNotContainKey(0L);
        assertThat(endBlocksReceived).as("end marker must not survive").doesNotContain(0L);
    }

    /// The resent block must reach messaging whole: header and body, then end-of-block. Before the
    /// fix only a lone end-of-block was sent, no verification session was ever opened for the block,
    /// and it was never retried.
    @Test
    @DisplayName("a block resent after a mid-block drop is forwarded whole, not as a lone end-of-block")
    void testResentBlockIsForwardedWholeAfterMidBlockDrop() {
        final TestBlock block = streamWholeBlockThenDrop(0L);
        final int sentBeforeResend = messagingFacility.getSentBlockItems().size();

        // Another publisher takes the resend and delivers the whole block.
        responsePipeline2.clear();
        publisherHandler2.onNext(block.asPublishStreamRequestUnparsed());
        endThisBlock(publisherHandler2, 0L);
        runForwarder();
        awaitForwarded(() -> forwardedFor(0L).stream().skip(sentBeforeResend).anyMatch(BlockItems::isEndOfBlock));

        final List<BlockItems> resent =
                forwardedFor(0L).subList(sentBeforeResend, forwardedFor(0L).size());
        assertThat(resent).as("the resend attempt must forward something").isNotEmpty();
        assertThat(resent.getFirst().isStartOfNewBlock())
                .as("the resend must begin with the block header, not a lone proof")
                .isTrue();
        assertThat(resent.getLast().isEndOfBlock())
                .as("the resend must finish with the end-of-block")
                .isTrue();
        assertThat(resent)
                .as("exactly one end-of-block for the resend")
                .filteredOn(BlockItems::isEndOfBlock)
                .hasSize(1);
    }

    /// Dropping a publisher mid-block clears state for that block only. A block another publisher
    /// has already streamed, with its proof collected, must still be forwarded whole afterwards.
    @Test
    @DisplayName("blockIsEnding() for one block leaves another block's collected proof alone")
    void testBlockIsEndingDoesNotDisturbOtherBlocks() {
        streamWholeBlockThenDrop(0L);

        // Publisher 2 streams block 1 and the forwarder collects its proof.
        final TestBlock block1 = TestBlockBuilder.generateBlockWithNumber(1L);
        publisherHandler2.onNext(block1.asPublishStreamRequestUnparsed());
        runForwarder();
        awaitForwarded(() -> forwardedFor(1L).stream().anyMatch(BlockItems::isStartOfNewBlock));

        // A drop for block 0 arrives again, as it can between handlers.
        toTest.blockIsEnding(0L, publisherHandlerId);
        endThisBlock(publisherHandler2, 1L);
        runForwarder();
        awaitForwarded(() -> forwardedFor(1L).stream().anyMatch(BlockItems::isEndOfBlock));

        assertThat(forwardedFor(1L))
                .as("block 1 is forwarded whole, with exactly one end-of-block")
                .filteredOn(BlockItems::isEndOfBlock)
                .hasSize(1);
        assertThat(forwardedFor(1L).getFirst().isStartOfNewBlock()).isTrue();
    }

    /// Starting a new attempt at a block must discard a proof left by an earlier attempt even when
    /// the earlier queue is already gone from the map, so a proof stranded by any path is cleared.
    @Test
    @DisplayName("registerQueueForBlock() discards a stale proof even without a previous queue")
    void testRegisterQueueDiscardsStaleProofWithoutPreviousQueue() throws Exception {
        final Map<Long, BlockItemUnparsed> blockProofs = managerField("blockProofs");
        final Set<Long> endBlocksReceived = managerField("endBlocksReceived");
        final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
        // A proof and end marker stranded by an earlier attempt, with no queue for the block.
        blockProofs.put(0L, block.getHeaderUnparsed());
        endBlocksReceived.add(0L);

        publisherHandler.onNext(block.asPublishStreamRequestUnparsed());

        assertThat(blockProofs)
                .as("stale proof is discarded when a new queue is registered")
                .doesNotContainKey(0L);
        assertThat(endBlocksReceived).as("stale end marker is discarded").doesNotContain(0L);
        endThisBlock(publisherHandler, 0L);
        runForwarder();
        final List<BlockItems> forwarded = forwardedFor(0L);
        assertThat(forwarded.getFirst().isStartOfNewBlock())
                .as("the new attempt is forwarded from its header")
                .isTrue();
        assertThat(forwarded.getLast().isEndOfBlock()).isTrue();
    }

    /// Safety net: if a proof and end marker are ever collected while the block's own queue still
    /// holds unforwarded batches, the forwarder must not send the proof alone. It must drop the
    /// block and schedule it for resend so it is not silently lost.
    @Test
    @DisplayName("forwarder does not send a lone proof for a queue it never forwarded; it schedules a resend")
    void testForwarderSchedulesResendInsteadOfSendingLoneProof() throws Exception {
        final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0L);
        // The publisher streams the block; the forwarder has not run yet, so the queue is unforwarded.
        publisherHandler.onNext(block.asPublishStreamRequestUnparsed());
        // Corrupt the state the way a stale proof would: a proof and end marker with an unforwarded queue.
        final Map<Long, BlockItemUnparsed> blockProofs = managerField("blockProofs");
        final Set<Long> endBlocksReceived = managerField("endBlocksReceived");
        blockProofs.put(0L, block.getHeaderUnparsed());
        endBlocksReceived.add(0L);

        runForwarder();

        assertThat(forwardedFor(0L))
                .as("no lone end-of-block may be sent for a block whose header was never forwarded")
                .noneMatch(BlockItems::isEndOfBlock);
        final Set<Long> blocksToResend = managerField("blocksToResend");
        assertThat(blocksToResend).as("the block must be scheduled for resend").contains(0L);
        final Map<Long, ?> queueByBlockMap = managerField("queueByBlockMap");
        assertThat(queueByBlockMap).as("the inconsistent queue is dropped").doesNotContainKey(0L);
        assertThat(blockProofs).doesNotContainKey(0L);
        assertThat(endBlocksReceived).doesNotContain(0L);
    }

    @SuppressWarnings("all")
    private BlockNodeContext generateContext(
            final HistoricalBlockFacility historicalBlockFacility,
            final ThreadPoolManager threadPoolManager,
            final BlockMessagingFacility blockMessagingFacility) {
        final Configuration configuration = TestUtils.createTestConfiguration()
                .withConfigDataType(PublisherConfig.class)
                .build();
        final MetricRegistry metrics = TestUtils.createMetrics();
        final HealthFacility serverHealth = null;
        // Non-null facility required: the manager publishes the next expected block on every
        // successful next-unstreamed CAS and would otherwise NPE.
        final ApplicationStateFacility applicationStateFacility = new TestApplicationStateFacility();
        final ServiceLoaderFunction serviceLoader = null;
        return new BlockNodeContext(
                configuration,
                metrics,
                serverHealth,
                blockMessagingFacility,
                historicalBlockFacility,
                applicationStateFacility,
                serviceLoader,
                threadPoolManager,
                null);
    }
}
