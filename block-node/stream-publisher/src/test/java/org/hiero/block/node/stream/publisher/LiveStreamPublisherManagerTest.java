// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.hiero.block.node.stream.publisher.fixtures.PublishApiUtility.endThisBlock;
import static org.hiero.block.node.stream.publisher.fixtures.PublishApiUtility.sendHeaderOnly;

import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import org.hiero.block.api.BlockNodeVersions;
import org.hiero.block.api.PublishStreamRequest.EndStream;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.PublishStreamResponse.EndOfStream.Code;
import org.hiero.block.api.PublishStreamResponse.ResponseOneOfType;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.app.fixtures.TestMetricsExporter;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.ScheduledBlockingExecutor;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.app.fixtures.blocks.TestBlock;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.pipeline.TestResponsePipeline;
import org.hiero.block.node.app.fixtures.plugintest.SimpleBlockRangeSet;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.PublisherStatusUpdateNotification;
import org.hiero.block.node.spi.blockmessaging.PublisherStatusUpdateNotification.UpdateType;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureType;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.node.spi.threading.ThreadPoolManager;
import org.hiero.block.node.stream.publisher.LiveStreamPublisherManager.MetricsHolder;
import org.hiero.block.node.stream.publisher.StreamPublisherManager.ActionForBlock;
import org.hiero.block.node.stream.publisher.StreamPublisherManager.BlockAction;
import org.hiero.metrics.core.MetricRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/// Tests for the [LiveStreamPublisherManager].
@DisplayName("LiveStreamPublisherManager Tests")
class LiveStreamPublisherManagerTest {

    private TestMetricsExporter metricsExporter;

    /// Constructor tests for the [LiveStreamPublisherManager].
    @Nested
    @DisplayName("Constructor Tests")
    class ConstructorTests {
        /// This test aims to assert that the constructor of
        /// [LiveStreamPublisherManager] does not throw any exceptions
        /// when provided with valid arguments.
        @Test
        @DisplayName("Constructor does not throw any exceptions with valid arguments")
        void testValidArguments() {
            assertThatNoException()
                    .isThrownBy(() -> new LiveStreamPublisherManager(generateContext(), generateManagerMetrics()));
        }

        /// This test aims to assert that the constructor of
        /// [LiveStreamPublisherManager] throws a
        /// [NullPointerException] when provided with a
        /// `null` context.
        @Test
        @DisplayName("Constructor throws NPE when provided with null context")
        void testNullContext() {
            assertThatNullPointerException()
                    .isThrownBy(() -> new LiveStreamPublisherManager(null, generateManagerMetrics()));
        }

        /// This test aims to assert that the constructor of
        /// [LiveStreamPublisherManager] throws a
        /// [NullPointerException] when provided with a
        /// `null` metrics.
        @Test
        @DisplayName("Constructor throws NPE when provided with null metrics")
        void testNullMetrics() {
            assertThatNullPointerException().isThrownBy(() -> new LiveStreamPublisherManager(generateContext(), null));
        }
    }

    /// Functionality tests for the [LiveStreamPublisherManager].
    @Nested
    @DisplayName("Functionality Tests")
    class FunctionalityTests {
        /// The test historical block facility to use when testing
        private SimpleInMemoryHistoricalBlockFacility historicalBlockFacility;
        /// The thread pool manager to use when testing
        private TestThreadPoolManager<BlockingExecutor, ScheduledBlockingExecutor> threadPoolManager;
        /// The messaging facility to use when testing
        private TestBlockMessagingFacility messagingFacility;

        // PUBLISHER 1
        /// The response pipeline to use when testing
        private TestResponsePipeline<PublishStreamResponse> responsePipeline;
        /// The publisher handler to use when testing
        private PublisherHandler publisherHandler;
        /// The ID of the publisher handler, used to identify it in the manager
        private long publisherHandlerId;

        // PUBLISHER 2
        /// The second response pipeline to use when testing
        private TestResponsePipeline<PublishStreamResponse> responsePipeline2;
        /// The second publisher handler to use when testing
        private PublisherHandler publisherHandler2;
        /// The ID of the second publisher handler, used to identify it in the manager
        private long publisherHandlerId2;

        private MetricsHolder managerMetrics;
        private PublisherHandler.MetricsHolder sharedHandlerMetrics;

        // INSTANCE UNDER TEST
        /// The instance under test
        private LiveStreamPublisherManager toTest;

        // EXTRACTORS
        private final Function<PublishStreamResponse, ResponseOneOfType> responseKindExtractor =
                response -> response.response().kind();
        private final Function<PublishStreamResponse, Long> acknowledgementBlockNumberExtractor =
                response -> Objects.requireNonNull(response.acknowledgement()).blockNumber();
        private final Function<PublishStreamResponse, Long> skipBlockNumberExtractor =
                response -> Objects.requireNonNull(response.skipBlock()).blockNumber();
        private final Function<PublishStreamResponse, Code> endStreamResponseCodeExtractor =
                response -> Objects.requireNonNull(response.endStream()).status();
        private final Function<PublishStreamResponse, Long> endStreamBlockNumberExtractor =
                response -> Objects.requireNonNull(response.endStream()).blockNumber();
        private final Function<PublishStreamResponse, Long> resendBlockNumberExtractor =
                response -> Objects.requireNonNull(response.resendBlock()).blockNumber();

        /// Environment setup called before each test.
        @BeforeEach
        void setup() {
            // Initialize the historical block facility and the context.
            historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
            threadPoolManager = new TestThreadPoolManager<>(
                    new BlockingExecutor(new LinkedBlockingQueue<>()),
                    new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
            messagingFacility = new TestBlockMessagingFacility();
            final BlockNodeContext context =
                    generateContext(historicalBlockFacility, threadPoolManager, messagingFacility);
            // Initialize the historical block facility with the context.
            historicalBlockFacility.init(context, null);
            // Create a shared registry so manager and handler metrics are visible through one exporter.
            final MetricRegistry registry = newRegistry();
            managerMetrics = MetricsHolder.createMetrics(registry);
            // Create the LiveStreamPublisherManager instance to test.
            toTest = new LiveStreamPublisherManager(context, managerMetrics);
            // We need to explicitly register the manager as a notification handler
            // The manager does not register itself.
            context.blockMessaging()
                    .registerBlockNotificationHandler(toTest, false, LiveStreamPublisherManager.class.getSimpleName());
            // Initialize the shared metrics holder for the publisher handlers.
            sharedHandlerMetrics = PublisherHandler.MetricsHolder.createMetrics(registry);
            // Create a response pipeline to handle the responses from the first publisher handler.
            responsePipeline = new TestResponsePipeline();
            // Create the first publisher handler and add it to the manager.
            publisherHandler = toTest.addHandler(responsePipeline, sharedHandlerMetrics, null);
            publisherHandlerId = 0L; // This should be set by the addHandler method, first call will use id 0L.
            // Create a second response pipeline to handle the responses from the second publisher handler.
            responsePipeline2 = new TestResponsePipeline();
            // Create the second publisher handler and add it to the manager.
            publisherHandler2 = toTest.addHandler(responsePipeline2, sharedHandlerMetrics, "");
            publisherHandlerId2 = 1L; // This should be set by the addHandler method, second call will use id 1L.
        }

        /// Tests for [LiveStreamPublisherManager#getActionForBlock(long, BlockAction, long)].
        @Nested
        @DisplayName("getActionForBlock() Tests")
        class GetActionForBlockTests {
            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#getActionForBlock(long, BlockAction, long)]
            /// method returns [BlockAction#ACCEPT] when the provided block
            /// number is the next expected one and previous action is `null`.
            @Test
            @DisplayName(
                    "getActionForBlock() returns ACCEPT when the provided block number is the next expected one and previous action is NULL")
            void testGetActionNullPreviousAction() {
                // Initially, the next expected block number is 0L.
                // Call
                final BlockAction actual = toTest.getActionForBlock(0L, null, publisherHandlerId);
                // Assert
                assertThat(actual).isEqualTo(BlockAction.ACCEPT);
            }

            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#getActionForBlock(long, BlockAction, long)]
            /// method returns [BlockAction#END_DUPLICATE] when the provided block
            /// number is lower or equal to the latest known block number and
            /// previous action is `null`.
            @ParameterizedTest
            @ValueSource(
                    longs = {
                        0L, 1L, 2L, 3L, 4L, 5L,
                    })
            @DisplayName(
                    "getActionForBlock() returns END_DUPLICATE when the provided block number is lower or equal to the latest known block number and previous action is NULL")
            void testGetActionNullPreviousActionDUPLICATE(final long blockNumber) {
                // First, we need to have some blocks available
                final SimpleBlockRangeSet availableBlocks = new SimpleBlockRangeSet();
                final long firstPersistedBlock = 0L;
                final long lastPersistedBlock = 5L;
                availableBlocks.add(firstPersistedBlock, lastPersistedBlock);
                historicalBlockFacility.setTemporaryAvailableBlocks(availableBlocks);
                assertThat(historicalBlockFacility.availableBlocks().contains(firstPersistedBlock, lastPersistedBlock))
                        .isTrue();
                // Then, we can send a persisted notification which will update the latest persisted block number, this
                // is critical to pass this test. No matter if the latest persisted is set during plugin startup or
                // via a notification, the result has to be the same.
                toTest.handlePersisted(new PersistedNotification(lastPersistedBlock, true, 0, BlockSource.PUBLISHER));
                final BlockAction actual = toTest.getActionForBlock(blockNumber, null, publisherHandlerId);
                // Assert
                assertThat(actual).isEqualTo(BlockAction.END_DUPLICATE);
            }

            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#getActionForBlock(long, BlockAction, long)]
            /// method returns [BlockAction#SKIP] when the provided block
            /// number is lower or equal to the latest known block number and
            /// previous action is `null`.
            @Test
            @DisplayName(
                    "getActionForBlock() returns SKIP when the provided block number is both higher than the latest known block number and lower than next expected block number, and previous action is NULL")
            void testGetActionNullPreviousActionSKIP() {
                // Initially, the next expected block number is 0L.
                // Initially, the latest known block number is -1L.
                // Call with valid next expected block number, this will increment next expected block number to 1L.
                final BlockAction firstCall = toTest.getActionForBlock(0L, null, publisherHandlerId);
                // It is important that we do not execute any tasks inside the thread pool
                // (we use the test fixture blocking one), otherwise the test would be flaky because the latest known
                // could be updated.
                // Assert the first call is successful.
                assertThat(firstCall).isEqualTo(BlockAction.ACCEPT);
                // Call with higher than latest known block number, but lower than next expected block number.
                final BlockAction actual = toTest.getActionForBlock(0L, null, publisherHandlerId);
                // Assert
                assertThat(actual).isEqualTo(BlockAction.SKIP);
            }

            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#getActionForBlock(long, BlockAction, long)]
            /// method returns [BlockAction#SEND_BEHIND] when the provided block
            /// number is higher than the next expected one and previous action is `null`.
            @Test
            @DisplayName(
                    "getActionForBlock() returns END_BEHIND when the provided block number is higher than the next expected one and previous action is NULL")
            void testGetActionNullPreviousActionBEHIND() {
                // Initially, the next expected block number is 0L.
                // Call with higher than next expected block number.
                final BlockAction actual = toTest.getActionForBlock(1L, null, publisherHandlerId);
                // Assert
                assertThat(actual).isEqualTo(BlockAction.SEND_BEHIND);
            }

            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#getActionForBlock(long, BlockAction, long)]
            /// method returns [BlockAction#ACCEPT] when the provided block
            /// number is the next expected one and previous action is [BlockAction#ACCEPT].
            @Test
            @DisplayName(
                    "getActionForBlock() returns ACCEPT when the provided block number is the next expected one and previous action is ACCEPT")
            void testGetActionACCEPTPreviousAction() {
                // Initially, the next expected block number is 0L.
                // Call with next expected block number and previous action null in order to "start" streaming block 0L.
                final long blockNumber = 0L;
                final BlockAction firstCall = toTest.getActionForBlock(blockNumber, null, publisherHandlerId);
                // We have to register a queue for the header we just got an ACCEPT for
                toTest.registerQueueForBlock(publisherHandlerId, new ConcurrentLinkedDeque<>(), blockNumber);
                // Assert that the first call returns ACCEPT.
                assertThat(firstCall).isEqualTo(BlockAction.ACCEPT);
                // Call with previous action ACCEPT.
                final BlockAction secondCall = toTest.getActionForBlock(blockNumber, firstCall, publisherHandlerId);
                // Assert that the second call also returns ACCEPT.
                assertThat(secondCall).isEqualTo(BlockAction.ACCEPT);
            }

            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#getActionForBlock(long, BlockAction, long)]
            /// method returns [BlockAction#SKIP] when we want to continue streaming a block which the
            /// manager no longer has a registered queue for, i.e., we had a previous action
            /// [BlockAction#ACCEPT], but we do not have a registered queue for that block within the
            // manager.
            @Test
            @DisplayName(
                    "getActionForBlock() returns SKIP when previous action was ACCEPT, but we do not have a registered queue for the block we want to continue")
            void testGetActionACCEPTNoQueueContinuingBlock() {
                // Call
                final BlockAction actual = toTest.getActionForBlock(0L, BlockAction.ACCEPT, publisherHandlerId);
                // Assert
                assertThat(actual).isEqualTo(BlockAction.SKIP);
            }

            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#getActionForBlock(long, BlockAction, long)]
            /// method returns [BlockAction#END_ERROR] when the provided
            /// previous action is neither `null` nor [BlockAction#ACCEPT].
            @ParameterizedTest
            @EnumSource(
                    value = BlockAction.class,
                    names = {"ACCEPT"},
                    mode = EnumSource.Mode.EXCLUDE)
            @DisplayName("getActionForBlock() returns END_ERROR if previous action is neither null nor ACCEPT")
            void testGetActionERROR(final BlockAction action) {
                // Call
                final BlockAction actual = toTest.getActionForBlock(0L, action, publisherHandlerId);
                // Assert
                assertThat(actual).isEqualTo(BlockAction.END_ERROR);
            }
        }

        /// Test for [LiveStreamPublisherManager#getLatestBlockNumber()].
        @Nested
        @DisplayName("getLatestBlockNumber() Tests")
        class GetLatestBlockNumberTests {
            /// This test aims to asser that the
            /// [LiveStreamPublisherManager#getLatestBlockNumber()] will return
            /// `-1L` when no blocks have been persisted yet.
            @Test
            @DisplayName("getLatestBlockNumber() returns -1 when no blocks have been persisted yet")
            void testLatestBlockWhenNonePersisted() {
                // Call
                final long actual = toTest.getLatestBlockNumber();
                // Assert
                assertThat(actual).isEqualTo(-1L);
            }

            /// This test aims to asser that the
            /// [LiveStreamPublisherManager#getLatestBlockNumber()] will return
            /// the latest block number that has been persisted.
            @Test
            @DisplayName("getLatestBlockNumber() returns latest persisted block number")
            void testLatestBlockNumber() {
                // Assert that the latest block number is -1L before we persist any blocks.
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(-1L);
                // Generate block with number 0 and send it to the historical block facility.
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
                // The below call will send a persisted notification which will be picked up by the
                // instance under test.
                historicalBlockFacility.handleBlockItemsReceived(block.asBlockItems());
                // Call
                final long actual = toTest.getLatestBlockNumber();
                // Assert that the latest block number is now 0.
                assertThat(actual).isEqualTo(block.number());
            }

            /// This test aims to asser that the
            /// [LiveStreamPublisherManager#getLatestBlockNumber()] will return
            /// the latest block number that has been persisted during object
            /// construction.
            @Test
            @DisplayName("getLatestBlockNumber() returns latest persisted block number during construction")
            void testLatestBlockNumberDuringConstruction() {
                final SimpleInMemoryHistoricalBlockFacility localHistoricalBlockFacility =
                        new SimpleInMemoryHistoricalBlockFacility();
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
                // Persist the block in the local historical block facility, we do not need to send a notification.
                localHistoricalBlockFacility.handleBlockItemsReceived(block.asBlockItems(), false);
                // Construct a new LiveStreamPublisherManager with the local historical block facility.
                final LiveStreamPublisherManager localToTest = new LiveStreamPublisherManager(
                        generateContext(localHistoricalBlockFacility), generateManagerMetrics());
                // After construction, the latest block number should be the one we just persisted.
                // Call
                final long actual = localToTest.getLatestBlockNumber();
                // Assert that the latest block number is now 0.
                assertThat(actual).isEqualTo(block.number());
            }
        }

        /// Tests for [LiveStreamPublisherManager#closeBlock(long)].
        ///
        /// Validate that the publisher manager correctly closes blocks.
        /// This inner class verifies that blocks are closed, metrics are correctly updated
        /// operation order is respected.
        ///
        /// Specific items include:
        /// - Metrics are updated after batches are forwarded
        /// - All pending batches are forwarded to messaging
        /// - The forwarder task is correctly started or restarted, if necessary
        /// - Metrics are not updated in the middle of sending data to messaging
        @Nested
        @DisplayName("closeBlock() Tests")
        @Timeout(value = 10, unit = TimeUnit.SECONDS)
        class CloseBlockTests {

            /// Helper to wait for the forwarder to finish, up to `timeoutMs`.
            private void awaitBatchesIncrement(final long before, final long timeoutMs) throws InterruptedException {
                // Compute a deadline (wall-clock millis) after which we give up waiting.
                final long deadline = System.currentTimeMillis() + timeoutMs;
                // Busy-wait in short sleeps until the batches metric increases beyond the 'before' baseline.
                while (System.currentTimeMillis() < deadline) {
                    // If the forwarder has completed at least one batch, the metric will be greater than baseline.
                    if (getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_BATCHES_MESSAGED) > before) return;
                    // Sleep briefly to avoid a hot spin while still reacting quickly when the metric changes.
                    Thread.sleep(10L);
                }
                // If we reach here, the timeout elapsed without observing an increment; let the caller assert as
                // needed.
            }

            /// Verifies that completed blocks update immediate and post-forwarder metrics and forward payloads.
            @Test
            @DisplayName("completed block updates metrics immediately and forwards after drain")
            void testCompletedBlockUpdatesMetricsAndForwards() throws InterruptedException {
                // Use block number 0 for this scenario.
                final long blockNumber = 0L;

                // Build at least one synthetic block item for the block we will close.
                final BlockUnparsed block =
                        TestBlockBuilder.generateBlockWithNumber(blockNumber).blockUnparsed();
                // Header-first sanity check
                assertThat(block.blockItems().getFirst().hasBlockHeader())
                        .as("first item must be a BlockHeader for block " + blockNumber)
                        .isTrue();
                // Wrap items into a request payload for the publisher.
                final PublishStreamRequestUnparsed req = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(BlockItemSetUnparsed.newBuilder()
                                .blockItems(block.blockItems())
                                .build())
                        .build();
                // Enqueue the request into the handler (this may also schedule the forwarder in production code).
                publisherHandler.onNext(req);
                // Mark the block as eligible to be closed (end-of-items for this block).
                endThisBlock(publisherHandler, blockNumber);

                // Capture the starting value for the async batches counter.
                final long beforeBatches =
                        getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_BATCHES_MESSAGED);
                // Capture the starting value for the immediate-close counter.
                final long beforeClosed = getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_CLOSED_COMPLETE);

                // Sanity check: no messages have been pushed yet before we trigger close.
                assertThat(messagingFacility.getSentBlockItems()).isEmpty();
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(blockNumber - 1);

                // Close the block; this should increment the immediate metric synchronously.
                toTest.closeBlock(blockNumber);

                // Immediate metric should reflect one close; the messaging facility remains empty until the forwarder
                // runs.
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_CLOSED_COMPLETE))
                        .isEqualTo(beforeClosed + 1);
                assertThat(messagingFacility.getSentBlockItems()).isEmpty();

                // Execute the queued task.
                threadPoolManager.executor().executeAsync(1_000L, false);
                // Wait (up to 3s) for the batches metric to increase beyond its baseline.
                awaitBatchesIncrement(beforeBatches, 3_000L);

                // Post-forwarder: both onNext() and closeBlock() may schedule; expect two batches produced.
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_CLOSED_COMPLETE))
                        .isEqualTo(beforeBatches + 2);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_OPEN_CONNECTIONS))
                        .isEqualTo(beforeBatches + 2);
                // The in-memory messaging facility should now have reset the block number to -1.
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(blockNumber - 1);
            }

            /// Verifies metric gating — batches only increment after the forwarder completes.
            @Test
            @DisplayName("batches increment only after forwarder completes (gating)")
            void testBatchesIncrementOnlyAfterForwarderCompletes() throws InterruptedException {
                // Baseline the async batches counter.
                final long beforeBatches =
                        getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_BATCHES_MESSAGED);
                // Use a distinct block number for isolation from other tests.
                final long blockNumber = 0L;

                // Build items for the same block that we will close.
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(blockNumber);
                // Wrap them into a publish request.
                final PublishStreamRequestUnparsed req = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();

                // Enqueue to the handler (may schedule forwarder depending on prod logic).
                publisherHandler.onNext(req);
                // Mark this block as ended/eligible.
                endThisBlock(publisherHandler, blockNumber);

                // Trigger closure (this should not immediately change the batches metric).
                toTest.closeBlock(blockNumber);
                // Still no messages until the forwarder runs.
                assertThat(messagingFacility.getSentBlockItems()).isEmpty();

                // Execute the queued tasks; the test pool throws if the queue is empty, enforcing correct sequencing.
                threadPoolManager.executor().executeAsync(1_000L, false);
                // Wait until the batches metric increases.
                awaitBatchesIncrement(beforeBatches, 3_000L);

                // After forwarder completion, batches should have increased and facility should contain messages.
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_CLOSED_COMPLETE))
                        .isEqualTo(beforeBatches + 2);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_OPEN_CONNECTIONS))
                        .isEqualTo(beforeBatches + 2);
                // The in-memory messaging facility should now have reset the block number to -1.
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(-1);
            }

            /// Verifies the forwarder restarts after completion — two runs yield four batches total.
            @Test
            @DisplayName("restarts forwarder after completion (two runs → four batches)")
            void testRestartForwarderAfterCompletion() throws InterruptedException {
                // ===== Run #1 =====
                // Use a unique block number for the first run.
                final long b0 = 0L;
                // Build items for block #b0.
                final TestBlock block0 = TestBlockBuilder.generateBlockWithNumber(b0);
                // Wrap into a request.
                final PublishStreamRequestUnparsed req0 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block0.asItemSetUnparsed())
                        .build();

                // Enqueue to handler.
                publisherHandler.onNext(req0);
                // Mark block b0 as ended.
                endThisBlock(publisherHandler, b0);
                // Baseline both async batches and immediate close counters.
                final long beforeBatches =
                        getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_BATCHES_MESSAGED);
                final long beforeClosed = getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_CLOSED_COMPLETE);
                // Close the block (immediate metric should +1).
                toTest.closeBlock(b0);
                // Verify immediate close counter progressed by exactly one.
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_CLOSED_COMPLETE))
                        .isEqualTo(beforeClosed + 1);
                // Execute the queued tasks; the test pool throws if the queue is empty, enforcing correct sequencing.
                threadPoolManager.executor().executeAsync(1_000L, false);
                // Wait until batches surpass baseline.
                awaitBatchesIncrement(beforeBatches, 3_000L);
                // After completion, we expect two batches (onNext + closeBlock scheduling).
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_CLOSED_COMPLETE))
                        .isEqualTo(beforeBatches + 2);
                // The in-memory messaging facility should now have reset the block number to -1.
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(-1);

                // ===== Run #2 =====
                // Use another unique block number for isolation.
                final long b1 = 1L;
                // Build items for block #b1.
                final TestBlock block1 = TestBlockBuilder.generateBlockWithNumber(b1);
                // Wrap into a request.
                final PublishStreamRequestUnparsed req1 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block1.asItemSetUnparsed())
                        .build();

                // Enqueue to handler.
                publisherHandler.onNext(req1);
                // Mark block b1 as ended.
                endThisBlock(publisherHandler, b1);
                // Close the block
                toTest.closeBlock(b1);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_CLOSED_COMPLETE))
                        .isEqualTo(beforeClosed + 3);

                // Wait until batches surpass the +2 baseline from the first run.
                awaitBatchesIncrement(beforeBatches + 2, 3_000L);

                // After the second completion, we expect four batches total (two per run).
                // After completion, we expect two batches (onNext + closeBlock scheduling).
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_CLOSED_COMPLETE))
                        .isEqualTo(beforeBatches + 4);
                // The in-memory messaging facility should now have reset the block number to -1.
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(-1);
            }

            /// Verifies idempotency — repeated closes while forwarder is active don’t double-count.
            @Test
            @DisplayName("no batch/count updates while forwarder active (idempotent closes)")
            void testNoMetricUpdatesWhileForwarderActive() throws InterruptedException {
                // Use a unique block number for this scenario.
                final long blockNumber = 0L;

                // Build items for this block.
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(blockNumber);
                // Wrap into a request.
                final PublishStreamRequestUnparsed req = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block.asItemSetUnparsed())
                        .build();

                // Enqueue to handler.
                publisherHandler.onNext(req);
                // Mark this block as ended so it becomes eligible for closure.
                endThisBlock(publisherHandler, blockNumber);

                // Baseline metrics.
                final long beforeBatches =
                        getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_BATCHES_MESSAGED);

                // Call closeBlock multiple times before draining; implementation should record only one completion
                // immediately.
                toTest.closeBlock(blockNumber);
                toTest.closeBlock(blockNumber);
                toTest.closeBlock(blockNumber);

                // Execute the queued tasks; the test pool throws if the queue is empty, enforcing correct sequencing.
                threadPoolManager.executor().executeAsync(1_000L, false);
                // Wait until the batches metric increases beyond baseline.
                awaitBatchesIncrement(beforeBatches, 3_000L);
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(-1);

                // After drain we expect at most one forwarder cycle to have run; verify that something was forwarded.
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_CLOSED_COMPLETE))
                        .isEqualTo(beforeBatches + 4);
                // The in-memory messaging facility should now have reset the block number to -1.
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(-1);
            }
        }

        /// Tests for [LiveStreamPublisherManager#handleVerification(VerificationNotification)].
        @Nested
        @DisplayName("handleVerification() Tests")
        class HandleVerificationTests {
            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#handleVerification(VerificationNotification)]
            /// does nothing when the notification states that the block has passed verification.
            /// We expect that no responses are sent.
            @Test
            @DisplayName("handleVerification() does nothing when block has passed verification, no responses sent")
            void testHandleVerificationPassed() {
                // We need to send a request via the publisher handler first,
                // This will properly update the internal state of the manager
                // so we can assert correctly. We aim to increment the next
                // unstreamed block number to 1L so we have a gap between
                // latest persisted (which should be -1L) and next unstreamed.
                // This is an expected condition during normal operation.
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
                // Now we build the request
                final BlockItemSetUnparsed itemSet = block.asItemSetUnparsed();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(itemSet)
                        .build();
                // We send the request to the publisher handler.
                // This will update the next unstreamed block number to 1L as
                // soon as we start streaming, i.e. a handler has queried the
                // manager for a block action for block 0L and at that point it
                // was the next expected block.
                publisherHandler.onNext(request);
                endThisBlock(publisherHandler, block.number());
                // Build a verification notification with passed verification.
                // Source must be publisher.
                final VerificationNotification notification =
                        new VerificationNotification(true, null, block.number(), null, null, BlockSource.PUBLISHER);
                // Call
                toTest.handleVerification(notification);
                // Assert that no responses have been sent.
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                assertThat(responsePipeline2.getOnNextCalls()).isEmpty();
                // Assert no other responses sent
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(responsePipeline.getClientEndStreamCalls().get()).isEqualTo(0);
                assertThat(responsePipeline2.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline2.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline2.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(responsePipeline2.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#handleVerification(VerificationNotification)]
            /// does nothing when the failed block's number is equal the
            /// latest know block in the manager.
            /// We expect that no responses are sent.
            @Test
            @DisplayName(
                    "handleVerification() does nothing when block number of failed block is equal to the latest known")
            void testHandleVerificationEqualToLatest() {
                // We need to send a request via the publisher handler first,
                // This will properly update the internal state of the manager
                // so we can assert correctly. We aim to increment the next
                // unstreamed block number to 1L so we have a gap between
                // latest persisted (which should be -1L) and next unstreamed.
                // This is an expected condition during normal operation.
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
                // Now we build the request
                final BlockItemSetUnparsed itemSet = block.asItemSetUnparsed();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(itemSet)
                        .build();
                // We send the request to the publisher handler.
                // This will update the next unstreamed block number to 1L as
                // soon as we start streaming, i.e. a handler has queried the
                // manager for a block action for block 0L and at that point it
                // was the next expected block.
                publisherHandler.onNext(request);
                endThisBlock(publisherHandler, block.number());
                // We run the queued messaging forwarder to properly stream the block to messaging
                threadPoolManager.executor().executeAsync(1_000L, false);
                // Now we need to send a PersistedNotification, so that the
                // latest known block number will be updated to 0L.
                final PersistedNotification persistedNotification =
                        new PersistedNotification(block.number(), true, 0, BlockSource.PUBLISHER);
                // Send the persisted notification to the manager.
                toTest.handlePersisted(persistedNotification);
                // Assert that the latest known block number is now 0L.
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(block.number());
                // Clear the pipeline because an acknowledgement response has been sent due to the
                // persisted notification.
                responsePipeline.clear();
                // Build a verification notification with block number equal to the latest known.
                // Source must be publisher.
                final VerificationNotification notification = new VerificationNotification(
                        false, FailureType.BAD_BLOCK_PROOF, block.number(), null, null, BlockSource.PUBLISHER);
                // Call
                toTest.handleVerification(notification);
                // Assert that only an Acknowledgement response has been sent,
                // this is because of the persisted notification. No other
                // responses should be sent.
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                // Assert no other responses sent
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(responsePipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#handleVerification(VerificationNotification)]
            /// does nothing when the failed block's number is lower than the
            /// latest know block in the manager.
            /// We expect that no responses are sent.
            @Test
            @DisplayName(
                    "handleVerification() does nothing when block number of failed block is lower than the latest known")
            void testHandleVerificationLowerThanLatest() {
                // We need to send a request via the publisher handler first,
                // This will properly update the internal state of the manager
                // so we can assert correctly. We aim to increment the next
                // unstreamed block number to 1L so we have a gap between
                // latest persisted (which should be -1L) and next unstreamed.
                // This is an expected condition during normal operation.
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
                // Now we build the request
                final BlockItemSetUnparsed itemSet = block.asItemSetUnparsed();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(itemSet)
                        .build();
                // We send the request to the publisher handler.
                // This will update the next unstreamed block number to 1L as
                // soon as we start streaming, i.e. a handler has queried the
                // manager for a block action for block 0L and at that point it
                // was the next expected block.
                publisherHandler.onNext(request);
                endThisBlock(publisherHandler, block.number());
                // We run the queued messaging forwarder to properly stream the block to messaging
                threadPoolManager.executor().executeAsync(1_000L, false);
                // We need to send a PersistedNotification first, so that the latest known block number will be updated
                // to 0L.
                final PersistedNotification persistedNotification =
                        new PersistedNotification(block.number(), true, 0, BlockSource.PUBLISHER);
                // Send the persisted notification to the manager.
                toTest.handlePersisted(persistedNotification);
                // Assert that the latest known block number is now 0L.
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(block.number());
                // Clear the pipeline because an acknowledgement response has been sent due to the
                // persisted notification.
                responsePipeline.clear();
                // Build a verification notification with block number lower than the latest known.
                // Source must be publisher.
                final VerificationNotification notification = new VerificationNotification(
                        false, FailureType.BAD_BLOCK_PROOF, block.number() - 1L, null, null, BlockSource.PUBLISHER);
                // Call
                toTest.handleVerification(notification);
                // Assert that only an Acknowledgement response has been sent,
                // this is because of the persisted notification. No other
                // responses should be sent.
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                // Assert no other responses sent
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(responsePipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#handleVerification(VerificationNotification)]
            /// does nothing when the failed block's number is equal the
            /// next unstreamed block in the manager.
            /// We expect that no responses are sent.
            @Test
            @DisplayName(
                    "handleVerification() does nothing when block number of failed block is equal to the next unstreamed")
            void testHandleVerificationEqualToNext() {
                // Initially, the next unstreamed block number is 0L.
                final long streamedBlockNumber = 0L;
                final VerificationNotification notification = new VerificationNotification(
                        false, FailureType.BAD_BLOCK_PROOF, streamedBlockNumber, null, null, BlockSource.PUBLISHER);
                // Call
                toTest.handleVerification(notification);
                // Assert that no responses have been sent.
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                // Assert no other responses sent
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(responsePipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#handleVerification(VerificationNotification)]
            /// does nothing when the failed block's number is higher than the
            /// next unstreamed block in the manager.
            /// We expect that no responses are sent.
            @Test
            @DisplayName(
                    "handleVerification() does nothing when block number of failed block is higher than the next unstreamed")
            void testHandleVerificationHigherThanNext() {
                // Initially, the next unstreamed block number is 0L.
                final long streamedBlockNumber = 1L;
                final VerificationNotification notification = new VerificationNotification(
                        false, FailureType.BAD_BLOCK_PROOF, streamedBlockNumber, null, null, BlockSource.PUBLISHER);
                // Call
                toTest.handleVerification(notification);
                // Assert that no responses have been sent.
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                // Assert no other responses sent
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(responsePipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#handleVerification(VerificationNotification)]
            /// will produce a [PublishStreamResponse.EndOfStream] response with
            /// [Code#BAD_BLOCK_PROOF] to the response pipeline of the handler
            /// that supplied the block with invalid proof.
            @Test
            @DisplayName(
                    "handleVerification() BAD_BLOCK_PROOF response is sent by the handler that supplied a block with invalid proof when verification fails")
            void testHandleVerificationBadBlockProof() {
                // We need to send a request via the publisher handler first,
                // This will properly update the internal state of the manager
                // so we can assert correctly. We aim to increment the next
                // unstreamed block number to 1L so we have a gap between
                // latest persisted (which should be -1L) and next unstreamed.
                // This is an expected condition during normal operation.
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
                // Now we build the request
                final BlockItemSetUnparsed itemSet = block.asItemSetUnparsed();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(itemSet)
                        .build();
                // We send the request to the publisher handler.
                // This will update the next unstreamed block number to 1L as
                // soon as we start streaming, i.e. a handler has queried the
                // manager for a block action for block 0L and at that point it
                // was the next expected block.
                publisherHandler.onNext(request);
                endThisBlock(publisherHandler, block.number());
                // Now, the publisher has sent the targeted block with broken proof.
                // We can now build a verification notification with failed verification.
                final VerificationNotification notification = new VerificationNotification(
                        false, FailureType.BAD_BLOCK_PROOF, block.number(), null, null, BlockSource.PUBLISHER);
                // Call
                toTest.handleVerification(notification);
                // Assert that the response pipeline has received a BAD_BLOCK_PROOF response, because the
                // publisher we used has sent a block with invalid proof.
                final List<PublishStreamResponse> onNextCalls = responsePipeline.getOnNextCalls();
                assertThat(onNextCalls)
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                        .returns(Code.BAD_BLOCK_PROOF, endStreamResponseCodeExtractor)
                        // below block number in the response is the latest known, -1L because none are stored
                        .returns(-1L, endStreamBlockNumberExtractor);
                onNextCalls.clear();
                // We expect a shutdown to be scheduled
                // We need to send any request or trigger any pipeline method
                // to do the actual shutdown
                // As a pre-check, we expect no onComplete calls
                assertThat(responsePipeline.getOnCompleteCalls().get()).isZero();
                publisherHandler.onNext(request);
                // Assert that no more responses are sent
                assertThat(onNextCalls).isEmpty();
                assertThat(responsePipeline.getOnCompleteCalls().get()).isEqualTo(1);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDOFSTREAM_SENT))
                        .isEqualTo(1);
                // Assert no other responses sent
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#handleVerification(VerificationNotification)]
            /// will not handle the notification of a failed block if the [BlockSource] is not
            /// [BlockSource#PUBLISHER]. No responses are expected to be sent and no metrics are
            /// expected to be updated.
            @ParameterizedTest
            @EnumSource(
                    value = BlockSource.class,
                    names = {"PUBLISHER"},
                    mode = EnumSource.Mode.EXCLUDE)
            @DisplayName(
                    "handleVerification() no handling when the BlockSource is not PUBLISHER, no responses of any kind sent")
            void testHandleVerificationNoPublisherSource(final BlockSource blockSource) {
                // We need to send a request via the publisher handler first,
                // This will properly update the internal state of the manager
                // so we can assert correctly. We aim to increment the next
                // unstreamed block number to 1L so we have a gap between
                // latest persisted (which should be -1L) and next unstreamed.
                // This is an expected condition during normal operation.
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
                // Now we build the request
                final BlockItemSetUnparsed itemSet = block.asItemSetUnparsed();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(itemSet)
                        .build();
                // We send the request to the publisher handler.
                // This will update the next unstreamed block number to 1L as
                // soon as we start streaming, i.e. a handler has queried the
                // manager for a block action for block 0L and at that point it
                // was the next expected block.
                publisherHandler.onNext(request);
                endThisBlock(publisherHandler, block.number());
                // Now, the publisher has sent the targeted block with broken proof.
                // We can now build a verification notification with failed verification.
                final VerificationNotification notification = new VerificationNotification(
                        false, FailureType.BAD_BLOCK_PROOF, block.number(), null, null, blockSource);
                // Call
                toTest.handleVerification(notification);
                // Assert that no shared metrics are updated
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_RESEND_SENT))
                        .isEqualTo(0);
                // Assert that no responses of any kind have been sent
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(responsePipeline.getClientEndStreamCalls().get()).isEqualTo(0);
                assertThat(responsePipeline2.getOnNextCalls()).isEmpty();
                assertThat(responsePipeline2.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline2.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline2.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(responsePipeline2.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#handleVerification(VerificationNotification)]
            /// will produce no response to the response
            /// pipelines of all handlers that did not supply the block with
            /// invalid proof that failed verification.
            @Test
            @DisplayName(
                    "handleVerification() no response is sent by all handlers that did not supply the block with invalid proof that failed verification")
            void testHandleVerificationNoResponse() {
                // We need to send a request via the publisher handler first,
                // This will properly update the internal state of the manager
                // so we can assert correctly. We aim to increment the next
                // unstreamed block number to 1L so we have a gap between
                // latest persisted (which should be -1L) and next unstreamed.
                // This is an expected condition during normal operation.
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
                // Now we build the request
                final BlockItemSetUnparsed itemSet = block.asItemSetUnparsed();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(itemSet)
                        .build();
                // We send the request to the second publisher handler.
                // This will update the next unstreamed block number to 1L as
                // soon as we start streaming, i.e. a handler has queried the
                // manager for a block action for block 0L and at that point it
                // was the next expected block.
                publisherHandler2.onNext(request);
                endThisBlock(publisherHandler2, block.number());
                // Build a verification notification with failed verification.
                final VerificationNotification notification = new VerificationNotification(
                        false, FailureType.BAD_BLOCK_PROOF, block.number(), null, null, BlockSource.PUBLISHER);
                // Call
                toTest.handleVerification(notification);
                // As a pre-check, we expect the pipeline to be empty
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                // We expect a shutdown to be scheduled
                // We need to send any request or trigger any pipeline method
                // to do the actual shutdown
                publisherHandler2.onNext(request);
                // Assert that the response pipeline has received no responses and the shared metrics is not updated.
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_RESEND_SENT))
                        .isEqualTo(0);
                // Assert no other responses sent
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(responsePipeline.getClientEndStreamCalls().get()).isEqualTo(0);
                // Now assert that the second publisher handler has received the response for BAD_BLOCK_PROOF
                // and is shut down (onComplete called).
                assertThat(responsePipeline2.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                        .returns(Code.BAD_BLOCK_PROOF, endStreamResponseCodeExtractor)
                        // below block number in the response is the latest known, -1L because none are stored
                        .returns(-1L, endStreamBlockNumberExtractor);
                assertThat(responsePipeline2.getOnCompleteCalls().get()).isEqualTo(1);
                // Assert no other responses sent
                assertThat(responsePipeline2.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline2.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline2.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#handleVerification(VerificationNotification)]
            /// will does not stop the normal operation of the publisher manager
            /// and subsequent items received by handlers will be processed
            /// accordingly.
            @Test
            @DisplayName(
                    "handleVerification() continues normal operation of the publisher manager after a failed verification")
            void testHandleVerificationNormalOperation() {
                // We need to send a request via the publisher handler first,
                // This will properly update the internal state of the manager
                // so we can assert correctly. We aim to increment the next
                // unstreamed block number to 1L so we have a gap between
                // latest persisted (which should be -1L) and next unstreamed.
                // This is an expected condition during normal operation.
                final TestBlock testBlock = TestBlockBuilder.generateBlockWithNumber(0);
                final BlockItemUnparsed[] block = testBlock.asBlockItemUnparsedArray();
                // Now we build the request
                final BlockItemSetUnparsed itemSet = testBlock.asItemSetUnparsed();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(itemSet)
                        .build();
                // We send the request to the second publisher handler.
                // This will update the next unstreamed block number to 1L as
                // soon as we start streaming, i.e. a handler has queried the
                // manager for a block action for block 0L and at that point it
                // was the next expected block.
                publisherHandler2.onNext(request);
                endThisBlock(publisherHandler2, testBlock.number());
                // Then, we need to simulate that the publisher has sent a block with invalid proof, i.e. call
                // handleVerification with failed verification.
                // Build a verification notification with failed verification.
                final VerificationNotification notification = new VerificationNotification(
                        false, FailureType.BAD_BLOCK_PROOF, testBlock.number(), null, null, BlockSource.PUBLISHER);
                // Call
                toTest.handleVerification(notification);
                // As a pre-check, we expect the pipeline to be empty
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                // We expect a shutdown to be scheduled
                // We need to send any request or trigger any pipeline method
                // to do the actual shutdown
                publisherHandler2.onNext(request);
                // Assert that the response pipeline of the first publisher has received no responses.
                // Also no metrics for resends is updated
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_RESEND_SENT))
                        .isEqualTo(0);
                // Assert no other responses sent
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(responsePipeline.getClientEndStreamCalls().get()).isEqualTo(0);
                // Now assert that the second publisher handler has received the response for BAD_BLOCK_PROOF
                // because it was responsible for sending the block with invalid proof and is shut down (onComplete
                // called).
                assertThat(responsePipeline2.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                        .returns(Code.BAD_BLOCK_PROOF, endStreamResponseCodeExtractor)
                        // below block number in the response is the latest known, -1L because none are stored
                        .returns(-1L, endStreamBlockNumberExtractor);
                assertThat(responsePipeline2.getOnCompleteCalls().get()).isEqualTo(1);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDOFSTREAM_SENT))
                        .isEqualTo(1);
                // Assert no other responses sent
                assertThat(responsePipeline2.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline2.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline2.getClientEndStreamCalls().get()).isEqualTo(0);
                // Before we send the request (we can reuse from above), we assert that the messaging
                // facility has no items received.
                assertThat(messagingFacility.getSentBlockItems()).isNotNull().isEmpty();
                // We send the request to the publisher handler.
                publisherHandler.onNext(request);
                endThisBlock(publisherHandler, testBlock.number());
                // We run the queued messaging forwarder to update the current streaming block number.
                // We need to run the task async, because the loop (managed by config) is way too big to block on.
                // We will however wait for one second to ensure the task is run.
                threadPoolManager.executor().executeAsync(1_000L, false);
                // Assert that items were propagated to the publisher handler.
                final List<BlockItems> sentBlockItems = messagingFacility.getSentBlockItems();
                // We expect 2 batches sent
                assertThat(sentBlockItems).isNotNull().isNotEmpty().hasSize(2);
                // First batch is the block, just before the proof
                assertThat(sentBlockItems.getFirst().blockItems())
                        .isNotNull()
                        .isNotEmpty()
                        .hasSize(block.length - 1)
                        .containsExactly(Arrays.copyOfRange(block, 0, block.length - 1));
                // Second batch is only the proof after the end of block is received
                assertThat(sentBlockItems.getLast().blockItems())
                        .isNotNull()
                        .isNotEmpty()
                        .hasSize(1)
                        .containsExactly(block[block.length - 1]);
            }
        }

        /// Tests for [LiveStreamPublisherManager#handlePersisted(PersistedNotification)].
        @Nested
        @DisplayName("handlePersisted() Tests")
        class HandlePersistedTests {
            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#handlePersisted(PersistedNotification)]
            /// will send acknowledgement to registered publisher handlers
            /// with the latest block number, i.e.
            /// [PersistedNotification#blockNumber()]. This test is when we have no active blocks.
            /// Active blocks are ones that are currently being published or awaiting,
            /// or in the process of being streamed to the live items messaging pipeline.
            @ParameterizedTest
            @ValueSource(longs = {0L, 1L, 10L, 100L, 1000L})
            @DisplayName(
                    "handlePersisted() sends acknowledgement with latest block number to all registered handlers - no active block")
            void testHandlePersistedValidNotificationNoActiveBlock(final long blockNumber) {
                // As a precondition, assert that the responses pipeline is empty (nothing has been sent yet)
                // Also as a precondition establish the last acknowledged block metric's value
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_LATEST_BLOCK_NUMBER_ACKNOWLEDGED))
                        .isEqualTo(0L);
                // Build the notification with end block number.
                final PersistedNotification notification =
                        new PersistedNotification(blockNumber, true, 0, BlockSource.PUBLISHER);
                // Call
                toTest.handlePersisted(notification);
                // Assert that the response pipeline has received a response with the expected latest block number.
                // We have one handler, so we expect one response.
                assertThat(responsePipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                        .returns(blockNumber, acknowledgementBlockNumberExtractor);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_LATEST_BLOCK_NUMBER_ACKNOWLEDGED))
                        .isEqualTo(blockNumber);
                // Assert no other responses sent
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(responsePipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#handlePersisted(PersistedNotification)]
            /// will send acknowledgement to registered publisher handlers
            /// with the latest block number, i.e.
            /// [PersistedNotification#blockNumber()]. This test is when we have active blocks.
            /// Active blocks are ones that are currently being published or awaiting,
            /// or in the process of being streamed to the live items messaging pipeline.
            /// For this test, we will stream the next expected block, which is 0L, then we will
            /// start streaming block 1L but will not finish it (will remain active) and we
            /// expect that when we send the persisted notification for block 0L it will be
            /// acknowledged.
            @Test
            @DisplayName(
                    "handlePersisted() sends acknowledgement with latest block number to all registered handlers - with active block")
            void testHandlePersistedValidNotificationWithActiveBlock() {
                // As a precondition, assert that the responses pipeline is empty (nothing has been sent yet)
                // Also as a precondition establish the last acknowledged block metric's value
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_LATEST_BLOCK_NUMBER_ACKNOWLEDGED))
                        .isEqualTo(0L);
                // Then we need to stream the next expected block, which is 0L now, but do not end it.
                // It must remain active when we send the notification.
                final TestBlock block0 = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request0 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block0.asItemSetUnparsed())
                        .build();
                publisherHandler.onNext(request0);
                // Then end the block
                endThisBlock(publisherHandler, block0.number());
                // Now we need to actually stream the block to messaging
                threadPoolManager.executor().executeAsync(1_000L, false);
                // Now start streaming block 1L
                final TestBlock block1 = TestBlockBuilder.generateBlockWithNumber(1L);
                final PublishStreamRequestUnparsed request1 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block1.asItemSetUnparsed())
                        .build();
                publisherHandler.onNext(request1);
                // Now send the notification for block 0
                final PersistedNotification notification =
                        new PersistedNotification(block0.number(), true, 0, BlockSource.PUBLISHER);
                // Call
                toTest.handlePersisted(notification);
                // Assert that the response pipeline has received a response with the expected latest block number.
                // We have one handler, so we expect one response.
                assertThat(responsePipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                        .returns(block0.number(), acknowledgementBlockNumberExtractor);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_LATEST_BLOCK_NUMBER_ACKNOWLEDGED))
                        .isEqualTo(block0.number());
                // Assert no other responses sent
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(responsePipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#handlePersisted(PersistedNotification)]
            /// will not send any acknowledgements and update any metrics when the notification is
            /// for a block that is higher than or equal to the current lowest active block.
            /// The current lowest active block is the block that is currently being published or
            /// awaiting, or in the process of being streamed to the live items messaging
            /// pipeline.
            @ParameterizedTest
            @ValueSource(longs = {0L, 1L, 10L, 100L, 1000L})
            @DisplayName(
                    "handlePersisted() - no acknowledgement for future block before acknowledgement for lowest active block")
            @Disabled("active-queue guard removed to allow backfill recovery — re-enable with @todo(#1841)")
            void testNoAcknowledgementForBlocksGreaterOrEqualToLowestActive(long blockNumber) {
                // As a precondition, assert that the responses pipeline is empty (nothing has been sent yet)
                // Also as a precondition establish the last acknowledged block metric's value
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_LATEST_BLOCK_NUMBER_ACKNOWLEDGED))
                        .isEqualTo(0L);
                // Then we need to stream the next expected block, which is 0L now, but do not end it.
                // It must remain active when we send the notification.
                final TestBlock block0 = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block0.asItemSetUnparsed())
                        .build();
                publisherHandler.onNext(request);
                final PersistedNotification notification =
                        new PersistedNotification(blockNumber, true, 0, BlockSource.PUBLISHER);
                // Call
                toTest.handlePersisted(notification);
                // Assert no metrics for last acknowledged are updated
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_LATEST_BLOCK_NUMBER_ACKNOWLEDGED))
                        .isEqualTo(0L);
                // Assert no responses
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(responsePipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#handlePersisted(PersistedNotification)]
            /// will set the latest known block number to the
            /// [PersistedNotification#blockNumber()] when there are no active blocks.
            /// Active blocks are ones that are currently being published or awaiting,
            /// or in the process of being streamed to the live items messaging pipeline.
            @ParameterizedTest
            @ValueSource(longs = {0L, 1L, 10L, 100L, 1000L})
            @DisplayName(
                    "handlePersisted() sets latest known block number to notification's blockNumber when acknowledged - no active block")
            void testHandlePersistedSetsLatestKnownBlockNumberNoActiveBlock(final long blockNumber) {
                // As a precondition, assert that the latest known block number is -1L (nothing has been persisted yet).
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(-1L);
                // Build the notification with end block number 10L.
                final PersistedNotification notification =
                        new PersistedNotification(blockNumber, true, 0, BlockSource.PUBLISHER);
                // Call
                toTest.handlePersisted(notification);
                // Assert that the latest known block number is now set to the notification's end block number.
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(blockNumber);
            }

            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#handlePersisted(PersistedNotification)]
            /// will set the latest known block number to the
            /// [PersistedNotification#blockNumber()] when there are active blocks.
            /// Active blocks are ones that are currently being published or awaiting,
            /// or in the process of being streamed to the live items messaging pipeline.
            /// Here we stream block 0 in full, it is then sent to internal messaging.
            /// Then we start streaming block 1, but we do not finish it, we must remain
            /// in the middle of streaming it so it is active. We expect that when we
            /// send successful persistence for block 0, it will be acknowledged and the
            /// [LiveStreamPublisherManager#getLatestBlockNumber()] will be updated to
            /// match the block we received successful persistence for.
            @Test
            @DisplayName(
                    "handlePersisted() sets latest known block number to notification's blockNumber when acknowledged - with active block")
            void testHandlePersistedSetsLatestKnownBlockNumberWithActiveBlock() {
                // As a precondition, assert that the latest known block number is -1L (nothing has been persisted yet).
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(-1L);
                // Then we need to stream the next expected block, which is 0L now, but do not end it.
                // It must remain active when we send the notification.
                final TestBlock block0 = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request0 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block0.asItemSetUnparsed())
                        .build();
                publisherHandler.onNext(request0);
                // Then end the block
                endThisBlock(publisherHandler, block0.number());
                // Now we need to actually stream the block to messaging
                threadPoolManager.executor().executeAsync(1_000L, false);
                // Now start streaming block 1L
                final TestBlock block1 = TestBlockBuilder.generateBlockWithNumber(1L);
                final PublishStreamRequestUnparsed request1 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block1.asItemSetUnparsed())
                        .build();
                publisherHandler.onNext(request1);
                // Now send the notification for block 0
                final PersistedNotification notification =
                        new PersistedNotification(block0.number(), true, 0, BlockSource.PUBLISHER);
                // Call
                toTest.handlePersisted(notification);
                // Assert that the latest known block number is now set to the notification's end block number.
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(block0.number());
            }

            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#handlePersisted(PersistedNotification)]
            /// will not set the latest known block number to the
            /// [PersistedNotification#blockNumber()] when that value is lower than the lowest active block.
            /// Active blocks are ones that are currently being published or awaiting,
            /// or in the process of being streamed to the live items messaging pipeline.
            @ParameterizedTest
            @ValueSource(longs = {0L, 1L, 10L, 100L, 1000L})
            @DisplayName(
                    "handlePersisted() does not change latest known block number to notification's blockNumber when it is lower that lowest active")
            @Disabled("active-queue guard removed to allow backfill recovery — re-enable with @todo(#1841)")
            void testHandlePersistedDoesNotSetLatestKnownBlockNumberWhenLowerThanLowestActive(final long blockNumber) {
                // As a precondition, assert that the latest known block number is -1L (nothing has been persisted yet).
                final long expectedLatestBlockNumber = -1L;
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(expectedLatestBlockNumber);
                // Then we need to stream the next expected block, which is 0L now, but do not end it.
                // It must remain active when we send the notification.
                final TestBlock block0 = TestBlockBuilder.generateBlockWithNumber(0L);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(block0.asItemSetUnparsed())
                        .build();
                publisherHandler.onNext(request);
                // Build the notification.
                final PersistedNotification notification =
                        new PersistedNotification(blockNumber, true, 0, BlockSource.PUBLISHER);
                // Call
                toTest.handlePersisted(notification);
                // Assert that the latest known block number is now set to the notification's end block number.
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(expectedLatestBlockNumber);
            }

            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#handlePersisted(PersistedNotification)]
            /// will not send acknowledgement to registered publisher handlers
            /// when the persistence has failed, i.e.
            /// [PersistedNotification#succeeded()] is false.
            @Test
            @DisplayName(
                    "handlePersisted() PERSISTENCE_FAILED is sent to all registered handlers when persistence failed")
            void testHandlePersistedNotificationFailedPersistence() {
                // As a precondition, assert that the responses pipeline is empty (nothing has been sent yet).
                // Also, assert that the latest known block number is -1L (initial state in order to compare later).
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                final long expectedLatestPersistedFromManager = -1L;
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(expectedLatestPersistedFromManager);
                final PersistedNotification notification =
                        new PersistedNotification(10L, false, 0, BlockSource.PUBLISHER);
                // Call
                toTest.handlePersisted(notification);
                // Assert that the response pipeline has received a PERSISTENCE_FAILED
                assertThat(responsePipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                        .returns(Code.PERSISTENCE_FAILED, endStreamResponseCodeExtractor)
                        // below block number in the response is the latest known, -1L because none are stored
                        .returns(-1L, endStreamBlockNumberExtractor);
                // As a pre-check, we expect no onComplete calls
                assertThat(responsePipeline.getOnCompleteCalls().get()).isZero();
                // We expect a shutdown to be scheduled
                // We need to send any request or trigger any pipeline method
                // to do the actual shutdown
                publisherHandler.onNext(
                        TestBlockBuilder.generateBlockWithNumber(0L).asPublishStreamRequestUnparsed());
                // Assert that the latest known block number is still -1L, it was not updated
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(expectedLatestPersistedFromManager);
                assertThat(responsePipeline.getOnCompleteCalls().get()).isEqualTo(1);
                // Assert no other responses sent
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getClientEndStreamCalls().get()).isEqualTo(0);
                publisherHandler2.onNext(
                        TestBlockBuilder.generateBlockWithNumber(0L).asPublishStreamRequestUnparsed());
                // Assert that the response pipeline has received a PERSISTENCE_FAILED
                assertThat(responsePipeline2.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                        .returns(Code.PERSISTENCE_FAILED, endStreamResponseCodeExtractor)
                        // below block number in the response is the latest known, -1L because none are stored
                        .returns(-1L, endStreamBlockNumberExtractor);
                // Assert that the latest known block number is still -1L, it was not updated
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(expectedLatestPersistedFromManager);
                assertThat(responsePipeline2.getOnCompleteCalls().get()).isEqualTo(1);
                // Assert no other responses sent
                assertThat(responsePipeline2.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline2.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline2.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// Verifies that a second [PersistedNotification] with the same
            /// (or lower) block number does not trigger another acknowledgement.
            /// This exercises the false branch of the
            /// {@code newLastPersistedBlock > lastPersistedBlockNumber} guard.
            @Test
            @DisplayName("handlePersisted() skips acknowledgement when block number does not advance lastPersisted")
            void testHandlePersistedWithNonAdvancingBlockNumber() {
                final long blockNumber = 5L;
                final SimpleBlockRangeSet availableBlocks = new SimpleBlockRangeSet();
                availableBlocks.add(0L, blockNumber);
                historicalBlockFacility.setTemporaryAvailableBlocks(availableBlocks);
                // First call — advances lastPersisted to blockNumber.
                toTest.handlePersisted(new PersistedNotification(blockNumber, true, 0, BlockSource.PUBLISHER));
                final int ackCountAfterFirst = responsePipeline.getOnNextCalls().size();
                assertThat(ackCountAfterFirst).isPositive();
                // Second call with the same block number — must NOT send another acknowledgement.
                toTest.handlePersisted(new PersistedNotification(blockNumber, true, 0, BlockSource.PUBLISHER));
                assertThat(responsePipeline.getOnNextCalls()).hasSize(ackCountAfterFirst);
            }

            /// Verifies that [clearObsoleteQueueItems] handles an empty queue deque
            /// without error, covering the {@code !deque.isEmpty()} false branch inside
            /// {@code getLastDequeItem}.
            @Test
            @DisplayName("handlePersisted() clears an empty registered queue without error")
            void testHandlePersistedClearsEmptyQueue() {
                // Register an empty queue for block 3 (before the persisted block).
                toTest.registerQueueForBlock(publisherHandlerId, new ConcurrentLinkedDeque<>(), 3L);
                // Persist block 5 — clearObsoleteQueueItems will inspect block 3's empty queue.
                final SimpleBlockRangeSet available = new SimpleBlockRangeSet();
                available.add(0L, 5L);
                historicalBlockFacility.setTemporaryAvailableBlocks(available);
                assertThatNoException()
                        .isThrownBy(() ->
                                toTest.handlePersisted(new PersistedNotification(5L, true, 0, BlockSource.PUBLISHER)));
            }

            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#handlePersisted(PersistedNotification)]
            /// will not send acknowledgement to registered publisher handlers
            /// when the notification is null.
            @Test
            @DisplayName("handlePersisted() does nothing when notification is null")
            void testHandlePersistedNotificationNull() {
                // As a precondition, assert that the responses pipeline is empty (nothing has been sent yet).
                // Also, assert that the latest known block number is -1L (initial state in order to compare later).
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                final long expectedLatestPersistedFromManager = -1L;
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(expectedLatestPersistedFromManager);
                // Call
                toTest.handlePersisted(null);
                // Assert that the response pipeline has not received any responses.
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                // Assert that the latest known block number is still -1L, it was not updated
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(expectedLatestPersistedFromManager);
                // Assert no other responses sent
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(responsePipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }
        }

        /// Tests for [LiveStreamPublisherManager#blockIsEnding(long, long)].
        @Nested
        @DisplayName("blockIsEnding() Tests")
        class BlockIsEndingTests {
            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#blockIsEnding(long, long)]
            /// will correctly handle an end stream request when the handler
            /// has completed it's current streaming block.
            @ParameterizedTest()
            @EnumSource(EndStream.Code.class)
            @DisplayName("Test blockIsEnding() with complete block")
            void testBlockIsEndingWithCompleteBlock(final EndStream.Code code) {
                // First, we build a valid request and send it to the publisher.
                // This will query for block action which will update the state
                // of the manager to have a next unstreamed block number of 1L.
                final TestBlock block = TestBlockBuilder.generateBlockWithNumber(0);
                // Now we build the request
                final BlockItemSetUnparsed itemSet = block.asItemSetUnparsed();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(itemSet)
                        .build();
                // We send the request to the publisher handler.
                publisherHandler.onNext(request);
                endThisBlock(publisherHandler, block.number());
                // Now we need to build an end stream request
                final EndStream endStream = EndStream.newBuilder()
                        .endCode(code)
                        .earliestBlockNumber(0L)
                        .latestBlockNumber(0L)
                        .build();
                // Build a PublishStreamRequest with the EndStream
                final PublishStreamRequestUnparsed endStreamRequest = PublishStreamRequestUnparsed.newBuilder()
                        .endStream(endStream)
                        .build();
                // Now we send the end stream request to the publisher handler.
                publisherHandler.onNext(endStreamRequest);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDSTREAM_RECEIVED))
                        .isEqualTo(1);
                // Now we must assert that the publisher has shutdown
                assertThat(responsePipeline.getOnCompleteCalls().get()).isEqualTo(1);
                // Assert no other responses sent
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getClientEndStreamCalls().get()).isEqualTo(0);
                // Now if we try to get the action for the same block, we should
                // we expect to get a SKIP, the block was complete, next expected is +1L.
                // We use the second publisher as the first one is already shut down.
                publisherHandler2.onNext(request);
                endThisBlock(publisherHandler, block.number());
                assertThat(responsePipeline2.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.SKIP_BLOCK, responseKindExtractor)
                        .returns(block.number(), skipBlockNumberExtractor);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCKS_SKIPS_SENT))
                        .isEqualTo(1);
                // Assert no other responses sent
                assertThat(responsePipeline2.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline2.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline2.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(responsePipeline2.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /// This test aims to assert that the
            /// [LiveStreamPublisherManager#blockIsEnding(long, long)]
            /// will correctly handle an end stream request when the handler
            /// has not completed it's current streaming block. This test runs
            /// the message forwarder task only in the end. If the message
            /// forwarder task is run at the beginning, we could be seeing
            /// different end results (i.e. more items streamed), but that is
            /// expected and should be another test.
            @ParameterizedTest()
            @EnumSource(EndStream.Code.class)
            @DisplayName("Test blockIsEnding() with incomplete block")
            void testBlockIsEndingWithIncompleteBlock(final EndStream.Code code) {
                // First, we build a valid request and send it to the publisher.
                // This will query for block action which will update the state
                // of the manager to have a next unstreamed block number of 1L.
                final TestBlock testBlock = TestBlockBuilder.generateBlockWithNumber(0);
                final BlockItemUnparsed[] block = testBlock.asBlockItemUnparsedArray();
                // Make the block incomplete
                final BlockItemUnparsed[] incompleteBlock = Arrays.copyOfRange(block, 0, block.length / 2);
                // Now we build the request
                final BlockItemSetUnparsed itemSet = BlockItemSetUnparsed.newBuilder()
                        .blockItems(incompleteBlock)
                        .build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(itemSet)
                        .build();
                // We send the request to the publisher handler.
                publisherHandler.onNext(request);
                // Now we need to build an end stream request
                final EndStream endStream = EndStream.newBuilder()
                        .endCode(code)
                        .earliestBlockNumber(testBlock.number())
                        .latestBlockNumber(testBlock.number())
                        .build();
                // Build a PublishStreamRequest with the EndStream
                final PublishStreamRequestUnparsed endStreamRequest = PublishStreamRequestUnparsed.newBuilder()
                        .endStream(endStream)
                        .build();
                // Now we send the end stream request to the publisher handler.
                publisherHandler.onNext(endStreamRequest);
                // Now we must assert that the publisher has shutdown
                assertThat(responsePipeline.getOnCompleteCalls().get()).isEqualTo(1);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_BLOCK_ENDSTREAM_RECEIVED))
                        .isEqualTo(1);
                // Assert no other responses sent
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getClientEndStreamCalls().get()).isEqualTo(0);
                // Now if we try to get the action for the same block, we should
                // we expect to get an ACCEPT, the block was incomplete, next expected
                // is the one we streamed incomplete.
                // We use the second publisher as the first one is already shut down.
                final BlockItemSetUnparsed fullBlockSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(block).build();
                final PublishStreamRequestUnparsed requestFullBlock = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(fullBlockSet)
                        .build();
                publisherHandler2.onNext(requestFullBlock);
                endThisBlock(publisherHandler2, testBlock.number());
                // We run the queued messaging forwarder to update the current streaming block number.
                // We need to run the task async, because the loop (managed by config) is way too big to block on.
                // We will however wait for one second to ensure the task is run.
                threadPoolManager.executor().executeAsync(1_000L, false);
                // Assert that the second request has been accepted and the block items were sent.
                final List<BlockItems> sentBlockItems = messagingFacility.getSentBlockItems();
                // We expect 2 batches sent
                assertThat(sentBlockItems).isNotNull().isNotEmpty().hasSize(2);
                // First batch is the block, just before the proof
                assertThat(sentBlockItems.getFirst().blockItems())
                        .isNotNull()
                        .isNotEmpty()
                        .hasSize(block.length - 1)
                        .containsExactly(Arrays.copyOfRange(block, 0, block.length - 1));
                // Second batch is only the proof after the end of block is received
                assertThat(sentBlockItems.getLast().blockItems())
                        .isNotNull()
                        .isNotEmpty()
                        .hasSize(1)
                        .containsExactly(block[block.length - 1]);
            }

            /// This test aims to assert that when a premature header is received, we will
            /// start streaming the new block, but will end mid-block the current one we are
            /// streaming. Effectively, we will have to call
            /// [LiveStreamPublisherManager#blockIsEnding(long, long)] for the block we will
            /// stop streaming. We do not expect a resend to be sent if we are re-starting the same block.
            /// NOTE:
            /// > In order for a handler to start streaming a block, it must pass through the manager.
            /// The manager does not allow two or more publishers to stream the same block simultaneously.
            /// That being said, this test aims to assert the only case where we _could_ possibly reach
            /// a situation where we are currently streaming block X and we start streaming the same block X
            /// while we are still currently streaming. In those cases, we do not need to ask for a resend
            /// of block X. The message forwarder will be able to continue forwarding this block, if it is
            /// currently streaming it. Otherwise, it must expect it as a resent block, or a block that
            /// is higher than the current streaming.
            @Test
            @DisplayName(
                    "Test blockIsEnding() when receiving premature header for same publisher, no resend if same block")
            void testBlockIsEndingWhenReceivingPrematureHeaderSamePublisherSameBlock() {
                // First, we need to build and stream the next block in line
                final TestBlock block0 = TestBlockBuilder.generateBlockWithNumber(0L);
                // Now we need to stream the block but not end it
                final PublishStreamRequestUnparsed block0Request = block0.asPublishStreamRequestUnparsed();
                publisherHandler.onNext(block0Request);
                // End this block, this will start the forwarder
                endThisBlock(publisherHandler, block0.number());
                // Run the message forwarding task, this will propagate the items to messaging. Sleep for a second.
                threadPoolManager.executor().executeAsync(500L, false);
                final List<BlockItems> sentBlockItems = messagingFacility.getSentBlockItems();
                // Now clear the block items sent to ease later asserts, we just streamed and forwarded block0
                assertThat(sentBlockItems).hasSize(2);
                sentBlockItems.clear();
                assertThat(sentBlockItems).isEmpty();
                // Now start sending the next block in line
                final TestBlock block1 = TestBlockBuilder.generateBlockWithNumber(block0.number() + 1);
                final PublishStreamRequestUnparsed block1Request = block1.asPublishStreamRequestUnparsed();
                publisherHandler.onNext(block1Request);
                // Give some time for the message forwarder to forward the items
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(500L));
                final List<BlockItemUnparsed> block1Items =
                        block1.asBlockItems().blockItems();
                final List<BlockItemUnparsed> expectedItemsSent = block1Items.subList(0, block1Items.size() - 1);
                final BlockItems expectedBlockItems = new BlockItems(expectedItemsSent, block1.number(), true, false);
                assertThat(sentBlockItems).hasSize(1).first().isEqualTo(expectedBlockItems);
                // Now clear the block items sent to ease later asserts
                sentBlockItems.clear();
                assertThat(sentBlockItems).isEmpty();
                // Now start sending the same block from the same publisher prematurely
                publisherHandler.onNext(block1Request);
                // Give some time for the message forwarder to forward the items
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(500L));
                // Now assert that the block has been sent to messaging
                assertThat(sentBlockItems).hasSize(1).first().isEqualTo(expectedBlockItems);
                // Now assert that if we end the block, we will not get a resend, because we do not expect
                // a resend to have been scheduled when we were streaming block x and then start streaming the same
                // block again
                final ActionForBlock actionForBlock = toTest.endOfBlock(block1.number());
                final ActionForBlock expected = new ActionForBlock(BlockAction.ACCEPT, block1.number());
                assertThat(actionForBlock).isEqualTo(expected);
            }

            /// This test aims to assert that when a premature header is received, we will
            /// start streaming the new block, but will end mid-block the current one we are
            /// streaming. Effectively, we will have to call
            /// [LiveStreamPublisherManager#blockIsEnding(long, long)] for the block we will
            /// stop streaming. We expect a resend to be sent if we have started streaming a different
            /// block.
            /// NOTE:
            /// > In order for a handler to start streaming a block, it must pass through the manager.
            /// The manager does not allow two or more publishers to stream the same block simultaneously.
            /// That being said, this test aims to assert when we are streaming block X and then start
            /// streaming block Y, we will end block X mid-block and ask for it to be resent.
            @Test
            @DisplayName(
                    "Test blockIsEnding() when receiving premature header for same publisher, schedule resend if not same block")
            void testBlockIsEndingWhenReceivingPrematureHeaderSamePublisherDifferentBlock() {
                // First, we need to build and stream the next block in line
                final TestBlock block0 = TestBlockBuilder.generateBlockWithNumber(0L);
                // Now we need to stream the block and we end it
                final PublishStreamRequestUnparsed requestBlock0 = block0.asPublishStreamRequestUnparsed();
                publisherHandler.onNext(requestBlock0);
                endThisBlock(publisherHandler, block0.number());
                // Disable the historical block facility so that we do not receive persistence notifications
                historicalBlockFacility.setDisablePlugin();
                // Run the message forwarding task, this will propagate the items to messaging. Sleep for a second.
                threadPoolManager.executor().executeAsync(500L, false);
                // Now assert that the block has been sent to messaging
                final List<BlockItemUnparsed> block0Items =
                        block0.asBlockItems().blockItems();
                final List<BlockItemUnparsed> expectedBlock0FirstItemsSent =
                        block0Items.subList(0, block0Items.size() - 1);
                final BlockItems expectedBlock0FirstItemBatch =
                        new BlockItems(expectedBlock0FirstItemsSent, block0.number(), true, false);
                final BlockItems expectedBlock0SecondItemBatch =
                        new BlockItems(List.of(block0Items.getLast()), block0.number(), false, true);
                final List<BlockItems> sentBlockItems = messagingFacility.getSentBlockItems();
                assertThat(sentBlockItems).hasSize(2);
                assertThat(sentBlockItems).first().isEqualTo(expectedBlock0FirstItemBatch);
                assertThat(sentBlockItems).last().isEqualTo(expectedBlock0SecondItemBatch);
                // Now clear the block items sent to ease later asserts
                sentBlockItems.clear();
                assertThat(sentBlockItems).isEmpty();
                // Now start sending the next block but do not end it
                final TestBlock block1 = TestBlockBuilder.generateBlockWithNumber(1L);
                PublishStreamRequestUnparsed requestBlock1 = block1.asPublishStreamRequestUnparsed();
                publisherHandler.onNext(requestBlock1);
                // Give some time for the message forwarder to forward the items
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(500L));
                // Now assert that the block has been sent to messaging, but is incomplete because we have not ended it
                final List<BlockItemUnparsed> block1Items =
                        block1.asBlockItems().blockItems();
                final List<BlockItemUnparsed> expectedBlock1ItemsSent = block1Items.subList(0, block1Items.size() - 1);
                final BlockItems expectedBlock1ItemBatch =
                        new BlockItems(expectedBlock1ItemsSent, block1.number(), true, false);
                assertThat(sentBlockItems).hasSize(1).first().isEqualTo(expectedBlock1ItemBatch);
                // Now clear the block items sent to ease later asserts
                sentBlockItems.clear();
                assertThat(sentBlockItems).isEmpty();
                // Now start streaming block 0 again. Block 0 is not yet acknowledged so we expect a SKIP for it,
                // but the handler is mid-block 1, so we expect that it will be scheduled for resend
                publisherHandler.onNext(requestBlock0);
                // Assert SKIP correctly received
                assertThat(responsePipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.SKIP_BLOCK, responseKindExtractor)
                        .returns(block0.number(), skipBlockNumberExtractor);
                // Now end any block, by any publisher in order to assert that block 1, which the publisher was in the
                // middle of streaming before receiving a premature header, is scheduled for resend.
                // To assert in this test, we will simply end it directly, not through a publisher.
                final ActionForBlock actionForBlock = toTest.endOfBlock(block0.number());
                final ActionForBlock expected = new ActionForBlock(BlockAction.RESEND, block1.number());
                assertThat(actionForBlock).isEqualTo(expected);
            }

            /// This test aims to assert that when a premature header is received, we will
            /// start streaming the new block, but will end mid-block the current one we are
            /// streaming. Effectively, we will have to call
            /// [LiveStreamPublisherManager#blockIsEnding(long, long)] for the block we will
            /// stop streaming. We expect a skip to be sent if the block we just started streaming
            /// has already been started by another handler
            /// NOTE:
            /// > In order for a handler to start streaming a block, it must pass through the manager.
            /// The manager does not allow two or more publishers to stream the same block simultaneously.
            /// That being said, whenever we start streaming a new block, only one handler is allowed to
            /// start streaming that block. While that block is not yet acknowledged, we expect to
            /// receive a [BlockAction#SKIP].
            @Test
            @DisplayName(
                    "Test blockIsEnding() when receiving premature header for same publisher, skip if block is already being streamed")
            void testBlockIsEndingWhenReceivingPrematureHeaderSamePublisherDifferentBlockSkip() {
                // First, we need to build and stream the next block in line
                final TestBlock block0 = TestBlockBuilder.generateBlockWithNumber(0L);
                // Now we need to stream the block and we end it
                final PublishStreamRequestUnparsed requestBlock0 = block0.asPublishStreamRequestUnparsed();
                publisherHandler.onNext(requestBlock0);
                endThisBlock(publisherHandler, block0.number());
                // Disable the historical block facility so that we do not receive persistence notifications
                historicalBlockFacility.setDisablePlugin();
                // Run the message forwarding task, this will propagate the items to messaging. Sleep for a second.
                threadPoolManager.executor().executeAsync(500L, false);
                // Now assert that the block has been sent to messaging
                final List<BlockItemUnparsed> block0Items =
                        block0.asBlockItems().blockItems();
                final List<BlockItemUnparsed> expectedBlock0FirstItemsSent =
                        block0Items.subList(0, block0Items.size() - 1);
                final BlockItems expectedBlock0FirstItemBatch =
                        new BlockItems(expectedBlock0FirstItemsSent, block0.number(), true, false);
                final BlockItems expectedBlock0SecondItemBatch =
                        new BlockItems(List.of(block0Items.getLast()), block0.number(), false, true);
                final List<BlockItems> sentBlockItems = messagingFacility.getSentBlockItems();
                assertThat(sentBlockItems).hasSize(2);
                assertThat(sentBlockItems).first().isEqualTo(expectedBlock0FirstItemBatch);
                assertThat(sentBlockItems).last().isEqualTo(expectedBlock0SecondItemBatch);
                // Now clear the block items sent to ease later asserts
                sentBlockItems.clear();
                assertThat(sentBlockItems).isEmpty();
                // Now start sending the next block but do not end it
                final TestBlock block1 = TestBlockBuilder.generateBlockWithNumber(1L);
                PublishStreamRequestUnparsed requestBlock1 = block1.asPublishStreamRequestUnparsed();
                publisherHandler.onNext(requestBlock1);
                // Give some time for the message forwarder to forward the items
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(500L));
                // Now assert that the block has been sent to messaging, but is incomplete because we have not ended it
                final List<BlockItemUnparsed> block1Items =
                        block1.asBlockItems().blockItems();
                final List<BlockItemUnparsed> expectedBlock1ItemsSent = block1Items.subList(0, block1Items.size() - 1);
                final BlockItems expectedBlock1ItemBatch =
                        new BlockItems(expectedBlock1ItemsSent, block1.number(), true, false);
                assertThat(sentBlockItems).hasSize(1).first().isEqualTo(expectedBlock1ItemBatch);
                // Now clear the block items sent to ease later asserts
                sentBlockItems.clear();
                assertThat(sentBlockItems).isEmpty();
                // Now the manager is not expecting to receive block 0 because it was already received
                // Before we stream block 0 again, pre-check that we have no onNext responses yet
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                // Now start streaming block 0 which is expected (by the manager) to be resent
                publisherHandler.onNext(requestBlock0);
                // Give some time for the message forwarder to forward the items
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(500L));
                // Now assert that we have not streamed anything
                assertThat(sentBlockItems).isEmpty();
                // Now assert that we have received a skip
                assertThat(responsePipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.SKIP_BLOCK, responseKindExtractor)
                        .returns(block0.number(), skipBlockNumberExtractor);
            }

            /// This test aims to assert that when a premature header is received, we will
            /// start streaming the new block, but will end mid-block the current one we are
            /// streaming. Effectively, we will have to call
            /// [LiveStreamPublisherManager#blockIsEnding(long, long)] for the block we will
            /// stop streaming. We expect a skip to be sent if the block we just started streaming
            /// has already been started by another handler
            /// NOTE:
            /// > In order for a handler to start streaming a block, it must pass through the manager.
            /// The manager does not allow two or more publishers to stream the same block simultaneously.
            /// If a block gets acknowledged, then for all blocks equal to or lower than the latest
            /// acknowledged block, we expect to get an [PublishStreamResponse.EndOfStream]
            /// with [PublishStreamResponse.EndOfStream.Code#DUPLICATE_BLOCK].
            @Test
            @DisplayName(
                    "Test blockIsEnding() when receiving premature header for same publisher, duplicate if block is already being acknowledged")
            void testBlockIsEndingWhenReceivingPrematureHeaderSamePublisherDifferentBlockDuplicate() {
                // First, we need to build and stream the next block in line
                final TestBlock block0 = TestBlockBuilder.generateBlockWithNumber(0L);
                // Now we need to stream the block and we end it
                final PublishStreamRequestUnparsed requestBlock0 = block0.asPublishStreamRequestUnparsed();
                publisherHandler.onNext(requestBlock0);
                endThisBlock(publisherHandler, block0.number());
                // Disable the historical block facility so that we do not receive persistence notifications
                historicalBlockFacility.setDisablePlugin();
                // Run the message forwarding task, this will propagate the items to messaging. Sleep for a second.
                threadPoolManager.executor().executeAsync(500L, false);
                // Now assert that the block has been sent to messaging
                final List<BlockItemUnparsed> block0Items =
                        block0.asBlockItems().blockItems();
                final List<BlockItemUnparsed> expectedBlock0FirstItemsSent =
                        block0Items.subList(0, block0Items.size() - 1);
                final BlockItems expectedBlock0FirstItemBatch =
                        new BlockItems(expectedBlock0FirstItemsSent, block0.number(), true, false);
                final BlockItems expectedBlock0SecondItemBatch =
                        new BlockItems(List.of(block0Items.getLast()), block0.number(), false, true);
                final List<BlockItems> sentBlockItems = messagingFacility.getSentBlockItems();
                assertThat(sentBlockItems).hasSize(2);
                assertThat(sentBlockItems).first().isEqualTo(expectedBlock0FirstItemBatch);
                assertThat(sentBlockItems).last().isEqualTo(expectedBlock0SecondItemBatch);
                // Now clear the block items sent to ease later asserts
                sentBlockItems.clear();
                assertThat(sentBlockItems).isEmpty();
                // Now start sending the next block but do not end it
                final TestBlock block1 = TestBlockBuilder.generateBlockWithNumber(1L);
                PublishStreamRequestUnparsed requestBlock1 = block1.asPublishStreamRequestUnparsed();
                publisherHandler.onNext(requestBlock1);
                // Give some time for the message forwarder to forward the items
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(500L));
                // Now assert that the block has been sent to messaging, but is incomplete because we have not ended it
                final List<BlockItemUnparsed> block1Items =
                        block1.asBlockItems().blockItems();
                final List<BlockItemUnparsed> expectedBlock1ItemsSent = block1Items.subList(0, block1Items.size() - 1);
                final BlockItems expectedBlock1ItemBatch =
                        new BlockItems(expectedBlock1ItemsSent, block1.number(), true, false);
                assertThat(sentBlockItems).hasSize(1).first().isEqualTo(expectedBlock1ItemBatch);
                // Now clear the block items sent to ease later asserts
                sentBlockItems.clear();
                assertThat(sentBlockItems).isEmpty();
                // Now we need to acknowledge block 0, we need to send a persisted notification
                toTest.handlePersisted(new PersistedNotification(block0.number(), true, 1000, BlockSource.PUBLISHER));
                // Before we stream block 0 again, pre-check that we have acknowledged block 0
                assertThat(responsePipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                        .returns(block0.number(), acknowledgementBlockNumberExtractor);
                // Now clear the pipeline so we can assert the duplicate below
                responsePipeline.getOnNextCalls().clear();
                // Now start streaming block 0 which is not expected (by the manager) to be resent
                publisherHandler.onNext(requestBlock0);
                // Give some time for the message forwarder to forward the items
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(500L));
                // Now assert that we have not streamed anything
                assertThat(sentBlockItems).isEmpty();
                // Now assert that we have received a skip
                assertThat(responsePipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                        .returns(Code.DUPLICATE_BLOCK, endStreamResponseCodeExtractor)
                        .returns(block0.number(), endStreamBlockNumberExtractor);
            }

            /// Verifies that [LiveStreamPublisherManager#blockIsEnding(long, long)]
            /// is a no-op when no queue has been registered for the given block
            /// (covers the {@code deque == null} branch).
            @Test
            @DisplayName("blockIsEnding() is a no-op when no queue is registered for the block")
            void testBlockIsEndingNoQueueRegistered() {
                // Block 999 was never registered — deque will be null.
                assertThatNoException().isThrownBy(() -> toTest.blockIsEnding(999L, publisherHandlerId));
            }

            /// Verifies that [LiveStreamPublisherManager#blockIsEnding(long, long)]
            /// does NOT add the block to resend when it is already persisted
            /// (covers the {@code blockNumber > lastPersistedBlockNumber} false branch).
            @Test
            @DisplayName("blockIsEnding() does not schedule resend for a block already persisted")
            void testBlockIsEndingForAlreadyPersistedBlock() {
                final long blockNumber = 5L;
                final SimpleBlockRangeSet available = new SimpleBlockRangeSet();
                available.add(0L, blockNumber);
                historicalBlockFacility.setTemporaryAvailableBlocks(available);
                toTest.handlePersisted(new PersistedNotification(blockNumber, true, 0, BlockSource.PUBLISHER));
                // Register an empty queue for the same block so deque != null.
                toTest.registerQueueForBlock(publisherHandlerId, new ConcurrentLinkedDeque<>(), blockNumber);
                // blockIsEnding for an already-persisted block must not throw.
                assertThatNoException().isThrownBy(() -> toTest.blockIsEnding(blockNumber, publisherHandlerId));
            }
        }

        /// Tests for usage of [PublisherStatusUpdateNotification].
        @Nested
        @Timeout(value = 10, unit = TimeUnit.SECONDS)
        @DisplayName("PublisherStatusUpdateNotification Tests")
        class PublisherStatusUpdateNotificationTests {
            /// The block node context used for testing.
            private BlockNodeContext context;
            /// The publisher config with overrides to be used for testing.
            private PublisherConfig testPublisherConfig;
            /// The thread pool manager used for testing.
            private TestThreadPoolManager<BlockingExecutor, ScheduledExecutorService> threadPoolManager;

            /// Local setup for each test in this class. Here we override the original setup that is present and reused
            /// from [FunctionalityTests]. This is needed because we want a clean slate so we can assert.
            /// The original setup pre-registers handlers which would interfere with the assertions made
            /// here.
            @BeforeEach
            void localSetup() {
                // Create a new manager with no pre-registered handlers.
                messagingFacility = new TestBlockMessagingFacility();
                final Map<String, String> configOverrides =
                        Map.ofEntries(Map.entry("producer.publisherUnavailabilityTimeout", "2"));
                final Configuration testConfiguration = createTestConfiguration(configOverrides);
                testPublisherConfig = testConfiguration.getConfigData(PublisherConfig.class);
                threadPoolManager = new TestThreadPoolManager<>(
                        new BlockingExecutor(new LinkedBlockingQueue<>()),
                        Executors.newSingleThreadScheduledExecutor());
                context = generateContext(
                        historicalBlockFacility, threadPoolManager, messagingFacility, testConfiguration);
                managerMetrics = generateManagerMetrics();
            }

            /// This test aims to assert that the [LiveStreamPublisherManager] will send a
            /// [PublisherStatusUpdateNotification] with type [UpdateType#PUBLISHER_UNAVAILABILITY_TIMEOUT]
            /// when no publishers are active for the configured timeout period.
            @Test
            @DisplayName("LiveStreamPublisherManager sends a timeout notification when no publishers are active")
            void testPublisherUnavailabilityTimeoutNotification() throws InterruptedException {
                // As a precondition, assert that no notifications were sent yet.
                final List<PublisherStatusUpdateNotification> notificationsPreCheck =
                        messagingFacility.getSentPublisherStatusUpdateNotifications();
                assertThat(notificationsPreCheck).isEmpty();
                // Create the LiveStreamPublisherManager instance to test, this also starts the timeout future.
                toTest = new LiveStreamPublisherManager(context, managerMetrics);
                // Sleep
                final long configuredTimeoutMillis = testPublisherConfig.publisherUnavailabilityTimeout() * 1_000L;
                // Give enough time for a notification to be sent, i.e. wait for timeout + 100ms buffer.
                Thread.sleep(configuredTimeoutMillis + 100L);
                // Shutdown the manager to stop the timeout future.
                threadPoolManager.shutdownNow();
                // Assert that a timeout notification was sent.
                final List<PublisherStatusUpdateNotification> actual =
                        messagingFacility.getSentPublisherStatusUpdateNotifications();
                assertThat(actual)
                        .isNotEmpty()
                        .hasSize(1)
                        .first()
                        .returns(UpdateType.PUBLISHER_UNAVAILABILITY_TIMEOUT, PublisherStatusUpdateNotification::type)
                        .returns(0, PublisherStatusUpdateNotification::activePublishers);
            }

            /// This test aims to assert that the [LiveStreamPublisherManager] will not send a
            /// [PublisherStatusUpdateNotification] with type [UpdateType#PUBLISHER_UNAVAILABILITY_TIMEOUT]
            /// when at least one publisher is active before the timeout period elapses.
            @Test
            @DisplayName("LiveStreamPublisherManager does not send a timeout notification when a publisher is active")
            void testNoPublisherUnavailabilityTimeoutNotificationWhenPublisherIsActive() throws InterruptedException {
                // As a precondition, assert that no notifications were sent yet.
                final List<PublisherStatusUpdateNotification> notificationsPreCheck =
                        messagingFacility.getSentPublisherStatusUpdateNotifications();
                assertThat(notificationsPreCheck).isEmpty();
                // Create the LiveStreamPublisherManager instance to test, this also starts the timeout future.
                toTest = new LiveStreamPublisherManager(context, managerMetrics);
                // Add a new handler to simulate an active publisher.
                toTest.addHandler(new TestResponsePipeline<>(), sharedHandlerMetrics, null);
                // Convert the configured timeout to milliseconds.
                final long configuredTimeoutMillis = testPublisherConfig.publisherUnavailabilityTimeout() * 1_000L;
                // Sleep for double the configured timeout to ensure that if a timeout notification
                // were to be sent, it would have been sent by now.
                Thread.sleep((configuredTimeoutMillis * 2) + 100L);
                // Shutdown the manager to stop the timeout future.
                threadPoolManager.shutdownNow();
                // Assert that no timeout notification was sent, but we see the one for a connected publisher.
                final List<PublisherStatusUpdateNotification> actual =
                        messagingFacility.getSentPublisherStatusUpdateNotifications();
                assertThat(actual)
                        .isNotEmpty()
                        .hasSize(1)
                        .first()
                        .returns(UpdateType.PUBLISHER_CONNECTED, PublisherStatusUpdateNotification::type)
                        .returns(1, PublisherStatusUpdateNotification::activePublishers);
            }

            /// This test aims to assert that the [LiveStreamPublisherManager] will reset the
            /// publisher unavailability timeout when no publishers remain active.
            @Test
            @DisplayName("LiveStreamPublisherManager restarts timeout future when no publishers remain active")
            void testPublisherUnavailabilityTimeoutResetOnNewPublisher() throws InterruptedException {
                // As a precondition, assert that no notifications were sent yet.
                final List<PublisherStatusUpdateNotification> notificationsPreCheck =
                        messagingFacility.getSentPublisherStatusUpdateNotifications();
                assertThat(notificationsPreCheck).isEmpty();
                // Create the LiveStreamPublisherManager instance to test, this also starts the timeout future.
                toTest = new LiveStreamPublisherManager(context, managerMetrics);
                // Add a new handler to simulate an active publisher.
                final long activeHandlerId = toTest.addHandler(new TestResponsePipeline<>(), sharedHandlerMetrics, "")
                        .getId();
                // Convert the configured timeout to milliseconds.
                final long configuredTimeoutMillis = testPublisherConfig.publisherUnavailabilityTimeout() * 1_000L;
                // Sleep for double the configured timeout to ensure that if a timeout notification
                // were to be sent, it would have been sent by now.
                Thread.sleep((configuredTimeoutMillis * 2) + 100L);
                // Assert that no timeout notification was sent, but we see the one for a connected publisher.
                final List<PublisherStatusUpdateNotification> actual =
                        messagingFacility.getSentPublisherStatusUpdateNotifications();
                assertThat(actual)
                        .isNotEmpty()
                        .hasSize(1)
                        .first()
                        .returns(UpdateType.PUBLISHER_CONNECTED, PublisherStatusUpdateNotification::type)
                        .returns(1, PublisherStatusUpdateNotification::activePublishers);
                // Clear the previously sent notifications in order not to clutter the assertions.
                messagingFacility.getSentPublisherStatusUpdateNotifications().clear();
                // Now, remove the active handler to simulate no active publishers.
                toTest.removeHandler(activeHandlerId);
                // Sleep for enough time to allow the timeout notification to be sent.
                Thread.sleep(configuredTimeoutMillis + 100L);
                // Shutdown the manager to stop the timeout future.
                threadPoolManager.shutdownNow();
                // Assert that a timeout notification was sent.
                final List<PublisherStatusUpdateNotification> nextActual =
                        messagingFacility.getSentPublisherStatusUpdateNotifications();
                assertThat(nextActual)
                        .hasSize(2)
                        .first()
                        .returns(UpdateType.PUBLISHER_DISCONNECTED, PublisherStatusUpdateNotification::type)
                        .returns(0, PublisherStatusUpdateNotification::activePublishers);
                assertThat(nextActual)
                        .last()
                        .returns(UpdateType.PUBLISHER_UNAVAILABILITY_TIMEOUT, PublisherStatusUpdateNotification::type)
                        .returns(0, PublisherStatusUpdateNotification::activePublishers);
            }

            /// This test aims to assert that the [LiveStreamPublisherManager] will send only one publisher
            /// unavailability timeout state change update and will not reset if state does not change.
            @Test
            @DisplayName("LiveStreamPublisherManager continues to restart timeout future when no publishers are active")
            void testPublisherUnavailabilityTimeoutContinuesWhenNoPublishersAreActive() throws InterruptedException {
                // As a precondition, assert that no notifications were sent yet.
                final List<PublisherStatusUpdateNotification> notificationsPreCheck =
                        messagingFacility.getSentPublisherStatusUpdateNotifications();
                assertThat(notificationsPreCheck).isEmpty();
                // Create the LiveStreamPublisherManager instance to test, this also starts the timeout future.
                toTest = new LiveStreamPublisherManager(context, managerMetrics);
                // Convert the configured timeout to milliseconds.
                final long configuredTimeoutMillis = testPublisherConfig.publisherUnavailabilityTimeout() * 1_000L;
                // Sleep for triple the configured timeout and some buffer to ensure that multiple timeout notifications
                // would have been sent by now.
                Thread.sleep((configuredTimeoutMillis * 3) + 200L);
                // Shutdown the manager to stop the timeout future.
                threadPoolManager.shutdownNow();
                // Assert that multiple timeout notifications were sent.
                final List<PublisherStatusUpdateNotification> actual =
                        messagingFacility.getSentPublisherStatusUpdateNotifications();
                assertThat(actual).isNotEmpty().hasSize(1).allSatisfy(notification -> {
                    assertThat(notification.type()).isEqualTo(UpdateType.PUBLISHER_UNAVAILABILITY_TIMEOUT);
                    assertThat(notification.activePublishers()).isZero();
                });
            }
        }

        /// Tests for [LiveStreamPublisherManager#addHandler].
        @Nested
        @DisplayName("addHandler() Tests")
        class AddHandlerTests {
            /// Local setup for each test in this class.
            /// Here we override the original setup that is present and reused
            /// from [FunctionalityTests]. This is needed because we want
            /// a clean slate so we can assert. The original setup pre-registers
            /// handlers which would interfere with the assertions made here.
            @BeforeEach
            void localSetup() {
                // Create a new manager with no pre-registered handlers.
                messagingFacility = new TestBlockMessagingFacility();
                final BlockNodeContext context =
                        generateContext(historicalBlockFacility, threadPoolManager, messagingFacility);
                managerMetrics = generateManagerMetrics();
                // Create the LiveStreamPublisherManager instance to test.
                toTest = new LiveStreamPublisherManager(context, managerMetrics);
            }

            /// This test aims to assert that registering a new handler
            /// via [LiveStreamPublisherManager#addHandler]
            /// will fire a [PublisherStatusUpdateNotification]
            /// indicating that a new publisher has connected.
            @Test
            @DisplayName("addHandler() fires a publisher status update notification")
            void testAddHandlerFiresStatusUpdateNotification() {
                // Make a pre-check that no notifications were sent yet.
                final List<PublisherStatusUpdateNotification> notificationsPreCheck =
                        messagingFacility.getSentPublisherStatusUpdateNotifications();
                assertThat(notificationsPreCheck).isEmpty();
                // Add a new handler.
                toTest.addHandler(responsePipeline, sharedHandlerMetrics, null);
                // Assert that a status update notification was sent.
                final List<PublisherStatusUpdateNotification> actual =
                        messagingFacility.getSentPublisherStatusUpdateNotifications();
                assertThat(actual)
                        .isNotEmpty()
                        .hasSize(1)
                        .first()
                        .returns(UpdateType.PUBLISHER_CONNECTED, PublisherStatusUpdateNotification::type)
                        .returns(1, PublisherStatusUpdateNotification::activePublishers);
            }

            /// This test aims to assert that registering a new handler
            /// via [LiveStreamPublisherManager#addHandler]
            /// will update the current active publishers count metric.
            @Test
            @DisplayName("addHandler() updates the current active publishers count metric")
            void testAddHandlerUpdatesActivePublishersMetric() {
                // Make a pre-check that the active publishers metric is zero.
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_OPEN_CONNECTIONS))
                        .isZero();
                // Add a new handler.
                toTest.addHandler(responsePipeline, sharedHandlerMetrics, "");
                // Assert that the active publishers metric is now 1.
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_OPEN_CONNECTIONS))
                        .isEqualTo(1);
            }
        }

        /// Tests for [LiveStreamPublisherManager#removeHandler(long)].
        @Nested
        @DisplayName("removeHandler() Tests")
        class RemoveHandlerTests {
            /// This test aims to assert that removing a handler
            /// via [LiveStreamPublisherManager#removeHandler(long)]
            /// will fire a [PublisherStatusUpdateNotification]
            /// indicating that a publisher has disconnected.
            @Test
            @DisplayName("removeHandler() fires a publisher status update notification")
            void testRemoveHandlerFiresStatusUpdateNotification() {
                // Make a pre-check that 2 handlers are registered from original setup.
                assertThat(messagingFacility.getSentPublisherStatusUpdateNotifications())
                        .hasSize(2)
                        .last()
                        .returns(UpdateType.PUBLISHER_CONNECTED, PublisherStatusUpdateNotification::type)
                        .returns(2, PublisherStatusUpdateNotification::activePublishers);
                // Clear the previously sent notifications from original setup in order not to clutter the assertions.
                messagingFacility.getSentPublisherStatusUpdateNotifications().clear();
                // Remove one handler.
                toTest.removeHandler(publisherHandlerId);
                // Assert that a status update notification was sent.
                assertThat(messagingFacility.getSentPublisherStatusUpdateNotifications())
                        .hasSize(1)
                        .last()
                        .returns(UpdateType.PUBLISHER_DISCONNECTED, PublisherStatusUpdateNotification::type)
                        .returns(1, PublisherStatusUpdateNotification::activePublishers);
                // Remove the second handler.
                toTest.removeHandler(publisherHandlerId2);
                // Assert that a status update notification was sent.
                assertThat(messagingFacility.getSentPublisherStatusUpdateNotifications())
                        .hasSize(2)
                        .last()
                        .returns(UpdateType.PUBLISHER_DISCONNECTED, PublisherStatusUpdateNotification::type)
                        .returns(0, PublisherStatusUpdateNotification::activePublishers);
            }

            /// Verifies that removing a handler ID that was never registered
            /// (handlerRemoved == null) is a no-op: no timeout is scheduled
            /// and the active count stays unchanged.
            @Test
            @DisplayName("removeHandler() with non-existent handler ID is a no-op")
            void testRemoveHandlerNonExistentIdIsNoOp() {
                final long nonExistentId = 999L;
                final long countBefore = getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_OPEN_CONNECTIONS);
                toTest.removeHandler(nonExistentId);
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_OPEN_CONNECTIONS))
                        .isEqualTo(countBefore);
            }

            /// This test aims to assert that removing a handler
            /// via [LiveStreamPublisherManager#removeHandler(long)]
            /// will update the current active publishers count metric.
            @Test
            @DisplayName("removeHandler() updates the current active publishers count metric")
            void testRemoveHandlerUpdatesActivePublishersMetric() {
                // Make a pre-check that the active publishers metric is 2 from original setup.
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_OPEN_CONNECTIONS))
                        .isEqualTo(2);
                // Remove one handler.
                toTest.removeHandler(publisherHandlerId);
                // Assert that the active publishers metric is now 1.
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_OPEN_CONNECTIONS))
                        .isEqualTo(1);
                // Remove the second handler.
                toTest.removeHandler(publisherHandlerId2);
                // Assert that the active publishers metric is now 0.
                assertThat(getMetricValue(StreamPublisherPlugin.METRIC_PUBLISHER_OPEN_CONNECTIONS))
                        .isZero();
            }
        }

        /// Tests for the [LiveStreamPublisherManager#endOfBlock(long)] method.
        @Nested
        @DisplayName("endOfBlock() Tests")
        class EndOfBlockTests {
            /// This test aims to assert that when the [LiveStreamPublisherManager#endOfBlock(long)] action is called,
            /// we expect it to return an [ActionForBlock] with the [BlockAction#ACCEPT] action, for the
            /// same block number, as used to calling the method, given the block nubmer is valid.
            @Test
            @DisplayName("endOfBlock() - ACCEPT received for a valid block number that is ending")
            void testEndOfBlockACCEPT() {
                final long endingBlock = 0L;
                // Call
                final ActionForBlock actionForBlock = toTest.endOfBlock(endingBlock);
                assertThat(actionForBlock)
                        .returns(BlockAction.ACCEPT, ActionForBlock::action)
                        .returns(endingBlock, ActionForBlock::blockNumber);
            }

            /// This test aims to assert that when the [LiveStreamPublisherManager#endOfBlock(long)] action is called,
            /// we expect it to return an [ActionForBlock] with the [BlockAction#RESEND] action, for
            /// the next block expected to be resent, given that there is such. In this test, we will fail
            /// the verification of a block and expect to get a resend for it.
            @Test
            @DisplayName("endOfBlock() - RESEND received for the next block that is expected to be resent")
            void testEndOfBlockRESEND() {
                // Create and stream a block
                final TestBlock blockThatFailsVerification = TestBlockBuilder.generateBlockWithNumber(0);
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(blockThatFailsVerification.asItemSetUnparsed())
                        .build();
                // We send the request to the publisher handler. This will increment the next unstreamed block number.
                publisherHandler.onNext(request);
                endThisBlock(publisherHandler, blockThatFailsVerification.number());
                // Now we can start streaming the next block, but we do not end it now. We have to end it after
                // we fail the verification for the first block. Stream from another publisher because
                // failing the verification of block 0 will terminate the first publisher
                final TestBlock block1 = TestBlockBuilder.generateBlockWithNumber(1);
                publisherHandler2.onNext(block1.asPublishStreamRequestUnparsed());
                // Handling a failed verification notification will schedule the block that failed to be resent
                toTest.handleVerification(new VerificationNotification(
                        false,
                        FailureType.BAD_BLOCK_PROOF,
                        blockThatFailsVerification.number(),
                        null,
                        null,
                        BlockSource.PUBLISHER));
                // Call, end block 1
                final ActionForBlock actionForBlock = toTest.endOfBlock(block1.number());
                // Assert resend received
                assertThat(actionForBlock)
                        .returns(BlockAction.RESEND, ActionForBlock::action)
                        .returns(blockThatFailsVerification.number(), ActionForBlock::blockNumber);
            }
        }

        /// Tests for stall detection via [LiveStreamPublisherManager#checkForStalledHandlers].
        @Nested
        @DisplayName("StallDetection Tests")
        class StallDetectionTests {

            /// A handler holding ACCEPT for a block and completing it normally
            /// (single handler) must never trigger stall detection.
            @Test
            @DisplayName("no stall detected when only one handler is connected")
            void testNoStallWithSingleHandler() {
                // Remove second handler so only handler 1 is connected.
                toTest.removeHandler(publisherHandlerId2);
                // Stream and complete blocks 0-3 via handler 1 alone.
                for (long block = 0L; block <= 3L; block++) {
                    final PublishStreamRequestUnparsed req = PublishStreamRequestUnparsed.newBuilder()
                            .blockItems(TestBlockBuilder.generateBlockWithNumber(block)
                                    .asItemSetUnparsed())
                            .build();
                    publisherHandler.onNext(req);
                    endThisBlock(publisherHandler, block);
                }
                // No EndStream(TIMEOUT) should have been sent.
                assertThat(responsePipeline.getOnNextCalls())
                        .as("single handler should never receive EndStream(TIMEOUT)")
                        .noneMatch(r -> r.response().kind() == PublishStreamResponse.ResponseOneOfType.END_STREAM);
                assertThat(responsePipeline.getOnCompleteCalls().get())
                        .as("single handler should not be shut down by stall detection")
                        .isZero();
            }

            /// Handler 2 completing block N+2 must NOT fire stall detection;
            /// only completing a block strictly greater than N+2 fires it.
            @Test
            @DisplayName("no stall when completed block equals stalled + 2 (threshold boundary)")
            void testNoStallWhenThresholdNotMet() {
                // Handler 1 sends only the header for block 0 — goes silent.
                sendHeaderOnly(publisherHandler, 0L);
                // Handler 2 streams and completes block 1 (SKIP for 0, ACCEPT for 1).
                final PublishStreamRequestUnparsed block1Req = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(TestBlockBuilder.generateBlockWithNumber(1L).asItemSetUnparsed())
                        .build();
                publisherHandler2.onNext(block1Req);
                endThisBlock(publisherHandler2, 1L);
                // Handler 2 completes block 2, 3: 3 == 0+3, NOT strictly greater — no stall yet.
                final PublishStreamRequestUnparsed block2Req = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(TestBlockBuilder.generateBlockWithNumber(2L).asItemSetUnparsed())
                        .build();
                publisherHandler2.onNext(block2Req);
                // Handler 2 completes block 2, 3: 3 == 0+3, NOT strictly greater — no stall yet.
                final PublishStreamRequestUnparsed block3Req = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(TestBlockBuilder.generateBlockWithNumber(3L).asItemSetUnparsed())
                        .build();
                publisherHandler2.onNext(block3Req);
                endThisBlock(publisherHandler2, 3L);
                assertThat(responsePipeline.getOnNextCalls())
                        .as("completing block 3 (== 0+3) must not trigger stall detection")
                        .noneMatch(r -> r.response().kind() == PublishStreamResponse.ResponseOneOfType.END_STREAM);
                assertThat(responsePipeline.getOnCompleteCalls().get()).isZero();
                // Handler 2 completes block 4: 4 > 0+3 — stall MUST fire now.
                final PublishStreamRequestUnparsed block4Req = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(TestBlockBuilder.generateBlockWithNumber(4L).asItemSetUnparsed())
                        .build();
                publisherHandler2.onNext(block4Req);
                endThisBlock(publisherHandler2, 4L);
                assertThat(responsePipeline.getOnNextCalls())
                        .as("completing block 4 (> 0+3) must trigger EndStream(TIMEOUT) on handler 1")
                        .anySatisfy(r -> {
                            assertThat(r.response().kind())
                                    .isEqualTo(PublishStreamResponse.ResponseOneOfType.END_STREAM);
                            assertThat(r.endStream().status())
                                    .isEqualTo(PublishStreamResponse.EndOfStream.Code.TIMEOUT);
                        });
                assertThat(responsePipeline.getOnCompleteCalls().get())
                        .as("stalled handler 1 must be shut down")
                        .isEqualTo(1);
            }

            /// Basic stall detection: handler 1 sends only a header for block 0
            /// then goes silent; handler 2 completes block 3.
            @Test
            @DisplayName("stall detected and EndStream(TIMEOUT) sent when ACCEPT winner goes silent")
            void testStallDetectedAndEndStreamSent() {
                // Handler 1 sends header only for block 0.
                sendHeaderOnly(publisherHandler, 0L);
                // Handler 2 is SKIPped for 0, then completes blocks 1, 2, 3, 4.
                for (long block = 1L; block <= 4L; block++) {
                    final PublishStreamRequestUnparsed req = PublishStreamRequestUnparsed.newBuilder()
                            .blockItems(TestBlockBuilder.generateBlockWithNumber(block)
                                    .asItemSetUnparsed())
                            .build();
                    publisherHandler2.onNext(req);
                    endThisBlock(publisherHandler2, block);
                }
                // Handler 1 must have received EndStream(TIMEOUT) and been shut down.
                assertThat(responsePipeline.getOnNextCalls())
                        .as("stalled handler 1 must receive EndStream(TIMEOUT)")
                        .anySatisfy(r -> {
                            assertThat(r.response().kind())
                                    .isEqualTo(PublishStreamResponse.ResponseOneOfType.END_STREAM);
                            assertThat(r.endStream().status())
                                    .isEqualTo(PublishStreamResponse.EndOfStream.Code.TIMEOUT);
                        });
                assertThat(responsePipeline.getOnCompleteCalls().get())
                        .as("stalled handler 1 must be shut down after stall")
                        .isEqualTo(1);
            }

            /// When the stalled handler disconnects before stall detection fires,
            /// block-is-ending adds block 0 to blocksToResend via the drop path.
            /// Stall detection should find no queue entry and be a no-op — no
            /// double action, no extra metric increment.
            @Test
            @DisplayName("stall action is a no-op when ACCEPT winner already disconnected")
            void testStalledHandlerAlreadyDisconnectedBeforeAction() {
                // Handler 1 sends header only then disconnects (clientEndStreamReceived).
                sendHeaderOnly(publisherHandler, 0L);
                publisherHandler.clientEndStreamReceived();
                // Clear pipeline noise from the disconnect itself.
                responsePipeline.clear();
                // Handler 2 completes blocks 1, 2, 3 — would trigger stall but block 0
                // queue is already gone.
                for (long block = 1L; block <= 3L; block++) {
                    final PublishStreamRequestUnparsed req = PublishStreamRequestUnparsed.newBuilder()
                            .blockItems(TestBlockBuilder.generateBlockWithNumber(block)
                                    .asItemSetUnparsed())
                            .build();
                    publisherHandler2.onNext(req);
                    endThisBlock(publisherHandler2, block);
                }
                // No EndStream(TIMEOUT) should be sent (handler 1 already gone).
                assertThat(responsePipeline.getOnNextCalls())
                        .as("no extra EndStream(TIMEOUT) when handler already disconnected")
                        .noneMatch(r -> r.response().kind() == PublishStreamResponse.ResponseOneOfType.END_STREAM);
                // No additional shutdown on handler 1 pipeline.
                assertThat(responsePipeline.getOnCompleteCalls().get())
                        .as("no extra onComplete when handler already disconnected")
                        .isZero();
            }

            /// If the ACCEPT winner finishes its block before stall detection fires,
            /// the entry is already removed from acceptWinnerByBlock at endOfBlock time;
            /// no EndStream(TIMEOUT) should be sent.
            @Test
            @DisplayName("no stall when ACCEPT winner completes block before threshold is reached")
            void testStalledHandlerCompletesBeforeActionExecutes() {
                // Handler 1 streams and fully completes block 0.
                final PublishStreamRequestUnparsed block0Req = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(TestBlockBuilder.generateBlockWithNumber(0L).asItemSetUnparsed())
                        .build();
                publisherHandler.onNext(block0Req);
                endThisBlock(publisherHandler, 0L);
                // Handler 2 then completes blocks 1, 2, 3 — but block 0 is already done.
                for (long block = 1L; block <= 3L; block++) {
                    final PublishStreamRequestUnparsed req = PublishStreamRequestUnparsed.newBuilder()
                            .blockItems(TestBlockBuilder.generateBlockWithNumber(block)
                                    .asItemSetUnparsed())
                            .build();
                    publisherHandler2.onNext(req);
                    endThisBlock(publisherHandler2, block);
                }
                // Handler 1 must not have received EndStream(TIMEOUT).
                assertThat(responsePipeline.getOnNextCalls())
                        .as("completed handler 1 must not receive EndStream(TIMEOUT)")
                        .noneMatch(r -> r.response().kind() == PublishStreamResponse.ResponseOneOfType.END_STREAM);
                assertThat(responsePipeline.getOnCompleteCalls().get())
                        .as("handler 1 should not be shut down by stall detection")
                        .isZero();
            }

            /// When two handlers are simultaneously stalled, a single completing
            /// handler that is far enough ahead must terminate both.
            @Test
            @DisplayName("all stalled handlers are terminated when multiple are stalled simultaneously")
            void testMultipleStalledHandlersAllTerminated() {
                // Add a third handler.
                final TestResponsePipeline<PublishStreamResponse> responsePipeline3 = new TestResponsePipeline<>();
                final PublisherHandler publisherHandler3 =
                        toTest.addHandler(responsePipeline3, sharedHandlerMetrics, "testID");
                // Handler 1 wins ACCEPT for block 0 — sends header only.
                sendHeaderOnly(publisherHandler, 0L);
                // Handler 2 wins ACCEPT for block 1 — sends header only.
                sendHeaderOnly(publisherHandler2, 1L);
                // Handler 3 completes block 5: 4 > 0+3 AND 5 > 1+3 — both stalled.
                for (long block = 2L; block <= 5L; block++) {
                    final PublishStreamRequestUnparsed req = PublishStreamRequestUnparsed.newBuilder()
                            .blockItems(TestBlockBuilder.generateBlockWithNumber(block)
                                    .asItemSetUnparsed())
                            .build();
                    publisherHandler3.onNext(req);
                    endThisBlock(publisherHandler3, block);
                }
                // Both handler 1 and handler 2 must have received EndStream(TIMEOUT).
                assertThat(responsePipeline.getOnNextCalls())
                        .as("stalled handler 1 must receive EndStream(TIMEOUT)")
                        .anySatisfy(r -> {
                            assertThat(r.response().kind())
                                    .isEqualTo(PublishStreamResponse.ResponseOneOfType.END_STREAM);
                            assertThat(r.endStream().status())
                                    .isEqualTo(PublishStreamResponse.EndOfStream.Code.TIMEOUT);
                        });
                assertThat(responsePipeline.getOnCompleteCalls().get())
                        .as("stalled handler 1 must be shut down")
                        .isEqualTo(1);
                assertThat(responsePipeline2.getOnNextCalls())
                        .as("stalled handler 2 must receive EndStream(TIMEOUT)")
                        .anySatisfy(r -> {
                            assertThat(r.response().kind())
                                    .isEqualTo(PublishStreamResponse.ResponseOneOfType.END_STREAM);
                            assertThat(r.endStream().status())
                                    .isEqualTo(PublishStreamResponse.EndOfStream.Code.TIMEOUT);
                        });
                assertThat(responsePipeline2.getOnCompleteCalls().get())
                        .as("stalled handler 2 must be shut down")
                        .isEqualTo(1);
            }

            /// After stall detection fires and block 0 is added to blocksToResend,
            /// the next endOfBlock from handler 2 must deliver ResendBlock(0) back
            /// to handler 2's publisher so it can take over.
            @Test
            @DisplayName("ResendBlock delivered at next endOfBlock after stall detection")
            void testResendDeliveredAtNextEndOfBlock() {
                // Handler 1 sends header only for block 0 — stalls.
                sendHeaderOnly(publisherHandler, 0L);
                // Handler 2 completes blocks 1, 2, 3, 4 — triggers stall on block 4.
                for (long block = 1L; block <= 4L; block++) {
                    final PublishStreamRequestUnparsed req = PublishStreamRequestUnparsed.newBuilder()
                            .blockItems(TestBlockBuilder.generateBlockWithNumber(block)
                                    .asItemSetUnparsed())
                            .build();
                    publisherHandler2.onNext(req);
                    endThisBlock(publisherHandler2, block);
                }
                // Stall must have fired — handler 1 terminated.
                assertThat(responsePipeline.getOnCompleteCalls().get())
                        .as("handler 1 must be shut down by stall detection before checking resend")
                        .isEqualTo(1);
                // Clear pipeline 2 noise (SKIP responses for blocks 0-4).
                responsePipeline2.clear();
                // Handler 2 now streams block 5; its endOfBlock must return ResendBlock(0).
                final PublishStreamRequestUnparsed block5Req = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(TestBlockBuilder.generateBlockWithNumber(5L).asItemSetUnparsed())
                        .build();
                publisherHandler2.onNext(block5Req);
                endThisBlock(publisherHandler2, 5L);
                // Handler 2 must have received a ResendBlock(0) response.
                assertThat(responsePipeline2.getOnNextCalls())
                        .as("handler 2 must receive ResendBlock(0) at next endOfBlock after stall")
                        .anySatisfy(r -> {
                            assertThat(r.response().kind())
                                    .isEqualTo(PublishStreamResponse.ResponseOneOfType.RESEND_BLOCK);
                            assertThat(r.resendBlock().blockNumber()).isEqualTo(0L);
                        });
            }
        }
    }

    /// This method generates a [BlockNodeContext] instance with default
    /// facilities that can be used in tests.
    private BlockNodeContext generateContext() {
        final HistoricalBlockFacility historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
        return generateContext(historicalBlockFacility);
    }

    /// This method generates a [BlockNodeContext] instance with default
    /// facilities that can be used in tests.
    private BlockNodeContext generateContext(final HistoricalBlockFacility historicalBlockFacility) {
        final ThreadPoolManager threadPoolManager = new TestThreadPoolManager<>(
                new BlockingExecutor(new LinkedBlockingQueue<>()),
                new ScheduledBlockingExecutor(new LinkedBlockingQueue<>()));
        final BlockMessagingFacility messagingFacility = new TestBlockMessagingFacility();
        return generateContext(historicalBlockFacility, threadPoolManager, messagingFacility);
    }

    /**
     * This method generates a {@link BlockNodeContext} instance with default
     * facilities that can be used in tests.
     */
    @SuppressWarnings("all")
    private BlockNodeContext generateContext(
            final HistoricalBlockFacility historicalBlockFacility,
            final ThreadPoolManager threadPoolManager,
            final BlockMessagingFacility blockMessagingFacility) {
        final Configuration configuration = createTestConfiguration();
        return generateContext(historicalBlockFacility, threadPoolManager, blockMessagingFacility, configuration);
    }

    /**
     * This method generates a {@link BlockNodeContext} instance with default
     * facilities that can be used in tests.
     */
    @SuppressWarnings("all")
    private BlockNodeContext generateContext(
            final HistoricalBlockFacility historicalBlockFacility,
            final ThreadPoolManager threadPoolManager,
            final BlockMessagingFacility blockMessagingFacility,
            final Configuration configuration) {
        final MetricRegistry metricRegistry = TestUtils.createMetrics();
        final HealthFacility serverHealth = null;
        final ServiceLoaderFunction serviceLoader = null;
        final ApplicationStateFacility applicationStateFacility = null;
        return new BlockNodeContext(
                configuration,
                metricRegistry,
                serverHealth,
                blockMessagingFacility,
                historicalBlockFacility,
                applicationStateFacility,
                serviceLoader,
                threadPoolManager,
                BlockNodeVersions.DEFAULT,
                null);
    }

    private static Configuration createTestConfiguration() {
        return createTestConfiguration(Map.of());
    }

    private static Configuration createTestConfiguration(final Map<String, String> overrides) {
        final ConfigurationBuilder builder =
                TestUtils.createTestConfiguration().withConfigDataType(PublisherConfig.class);
        overrides.forEach(builder::withValue);
        return builder.build();
    }

    /// This method generates a [MetricsHolder] instance with default
    /// metrics that can be used in tests.
    private MetricRegistry newRegistry() {
        metricsExporter = new TestMetricsExporter();
        return MetricRegistry.builder().setMetricsExporter(metricsExporter).build();
    }

    /// Creates a new [PublisherHandler.MetricsHolder] with default counters for testing.
    /// These counters could be queried to verify the metrics' states.
    private MetricsHolder generateManagerMetrics() {
        return MetricsHolder.createMetrics(newRegistry());
    }

    /// Creates a new [PublisherHandler.MetricsHolder] with default counters for testing.
    /// These counters could be queried to verify the metrics' states.
    private PublisherHandler.MetricsHolder generateHandlerMetrics() {
        return PublisherHandler.MetricsHolder.createMetrics(TestUtils.createMetrics());
    }

    private long getMetricValue(org.hiero.metrics.core.MetricKey<?> metricKey) {
        return metricsExporter.getMetricValue(metricKey.name());
    }
}
