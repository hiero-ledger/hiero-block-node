// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.hiero.block.node.stream.publisher.fixtures.PublishApiUtility.endThisBlock;

import com.swirlds.config.api.Configuration;
import com.swirlds.metrics.api.Metrics;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.hiero.block.api.PublishStreamRequest.EndStream;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.PublishStreamResponse.EndOfStream.Code;
import org.hiero.block.api.PublishStreamResponse.ResponseOneOfType;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.app.fixtures.TestUtils;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.app.fixtures.pipeline.TestResponsePipeline;
import org.hiero.block.node.app.fixtures.plugintest.SimpleBlockRangeSet;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockMessagingFacility;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.node.spi.threading.ThreadPoolManager;
import org.hiero.block.node.stream.publisher.LiveStreamPublisherManager.MetricsHolder;
import org.hiero.block.node.stream.publisher.StreamPublisherManager.BlockAction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for the {@link LiveStreamPublisherManager}.
 */
@DisplayName("LiveStreamPublisherManager Tests")
class LiveStreamPublisherManagerTest {
    /**
     * Constructor tests for the {@link LiveStreamPublisherManager}.
     */
    @Nested
    @DisplayName("Constructor Tests")
    class ConstructorTests {
        /**
         * This test aims to assert that the constructor of
         * {@link LiveStreamPublisherManager} does not throw any exceptions
         * when provided with valid arguments.
         */
        @Test
        @DisplayName("Constructor does not throw any exceptions with valid arguments")
        void testValidArguments() {
            assertThatNoException()
                    .isThrownBy(() -> new LiveStreamPublisherManager(generateContext(), generateManagerMetrics()));
        }

        /**
         * This test aims to assert that the constructor of
         * {@link LiveStreamPublisherManager} throws a
         * {@link NullPointerException} when provided with a
         * {@code null} context.
         */
        @Test
        @DisplayName("Constructor throws NPE when provided with null context")
        void testNullContext() {
            assertThatNullPointerException()
                    .isThrownBy(() -> new LiveStreamPublisherManager(null, generateManagerMetrics()));
        }

        /**
         * This test aims to assert that the constructor of
         * {@link LiveStreamPublisherManager} throws a
         * {@link NullPointerException} when provided with a
         * {@code null} metrics.
         */
        @Test
        @DisplayName("Constructor throws NPE when provided with null metrics")
        void testNullMetrics() {
            assertThatNullPointerException().isThrownBy(() -> new LiveStreamPublisherManager(generateContext(), null));
        }
    }

    /**
     * Functionality tests for the {@link LiveStreamPublisherManager}.
     */
    @Nested
    @DisplayName("Functionality Tests")
    class FunctionalityTests {
        /** The test historical block facility to use when testing */
        private SimpleInMemoryHistoricalBlockFacility historicalBlockFacility;
        /** The thread pool manager to use when testing */
        private TestThreadPoolManager<BlockingExecutor> threadPoolManager;
        /** The messaging facility to use when testing */
        private TestBlockMessagingFacility messagingFacility;

        // PUBLISHER 1
        /** The response pipeline to use when testing */
        private TestResponsePipeline<PublishStreamResponse> responsePipeline;
        /** The publisher handler to use when testing */
        private PublisherHandler publisherHandler;
        /** The ID of the publisher handler, used to identify it in the manager */
        private long publisherHandlerId;

        // PUBLISHER 2
        /** The second response pipeline to use when testing */
        private TestResponsePipeline<PublishStreamResponse> responsePipeline2;
        /** The second publisher handler to use when testing */
        private PublisherHandler publisherHandler2;
        /** The ID of the second publisher handler, used to identify it in the manager */
        private long publisherHandlerId2;

        private MetricsHolder managerMetrics;
        private PublisherHandler.MetricsHolder sharedHandlerMetrics;

        // INSTANCE UNDER TEST
        /** The instance under test */
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

        /**
         * Environment setup called before each test.
         */
        @BeforeEach
        void setup() {
            // Initialize the historical block facility and the context.
            historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
            threadPoolManager = new TestThreadPoolManager<>(new BlockingExecutor(new LinkedBlockingQueue<>()));
            messagingFacility = new TestBlockMessagingFacility();
            final BlockNodeContext context =
                    generateContext(historicalBlockFacility, threadPoolManager, messagingFacility);
            // Initialize the historical block facility with the context.
            historicalBlockFacility.init(context, null);
            // Create the metrics holder for the manager.
            managerMetrics = generateManagerMetrics();
            // Create the LiveStreamPublisherManager instance to test.
            toTest = new LiveStreamPublisherManager(context, managerMetrics);
            // We need to explicitly register the manager as a notification handler
            // The manager does not register itself.
            context.blockMessaging()
                    .registerBlockNotificationHandler(toTest, false, LiveStreamPublisherManager.class.getSimpleName());
            // Initialize the shared metrics holder for the publisher handlers.
            sharedHandlerMetrics = generateHandlerMetrics();
            // Create a response pipeline to handle the responses from the first publisher handler.
            responsePipeline = new TestResponsePipeline();
            // Create the first publisher handler and add it to the manager.
            publisherHandler = toTest.addHandler(responsePipeline, sharedHandlerMetrics);
            publisherHandlerId = 0L; // This should be set by the addHandler method, first call will use id 0L.
            // Create a second response pipeline to handle the responses from the second publisher handler.
            responsePipeline2 = new TestResponsePipeline();
            // Create the second publisher handler and add it to the manager.
            publisherHandler2 = toTest.addHandler(responsePipeline2, sharedHandlerMetrics);
            publisherHandlerId2 = 1L; // This should be set by the addHandler method, second call will use id 1L.
        }

        /**
         * Tests for {@link LiveStreamPublisherManager#getActionForBlock(long, BlockAction, long)}.
         */
        @Nested
        @DisplayName("getActionForBlock() Tests")
        class GetActionForBlockTests {
            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#getActionForBlock(long, BlockAction, long)}
             * method returns {@link BlockAction#ACCEPT} when the provided block
             * number is the next expected one and previous action is {@code null}.
             */
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

            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#getActionForBlock(long, BlockAction, long)}
             * method returns {@link BlockAction#END_DUPLICATE} when the provided block
             * number is lower or equal to the latest known block number and
             * previous action is {@code null}.
             */
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
                toTest.handlePersisted(new PersistedNotification(lastPersistedBlock, true, 0, BlockSource.UNKNOWN));
                final BlockAction actual = toTest.getActionForBlock(blockNumber, null, publisherHandlerId);
                // Assert
                assertThat(actual).isEqualTo(BlockAction.END_DUPLICATE);
            }

            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#getActionForBlock(long, BlockAction, long)}
             * method returns {@link BlockAction#SKIP} when the provided block
             * number is lower or equal to the latest known block number and
             * previous action is {@code null}.
             */
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

            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#getActionForBlock(long, BlockAction, long)}
             * method returns {@link BlockAction#END_BEHIND} when the provided block
             * number is higher than the next expected one and previous action is {@code null}.
             */
            @Test
            @DisplayName(
                    "getActionForBlock() returns END_BEHIND when the provided block number is higher than the next expected one and previous action is NULL")
            void testGetActionNullPreviousActionBEHIND() {
                // Initially, the next expected block number is 0L.
                // Call with higher than next expected block number.
                final BlockAction actual = toTest.getActionForBlock(1L, null, publisherHandlerId);
                // Assert
                assertThat(actual).isEqualTo(BlockAction.END_BEHIND);
            }

            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#getActionForBlock(long, BlockAction, long)}
             * method returns {@link BlockAction#ACCEPT} when the provided block
             * number is the next expected one and previous action is {@link BlockAction#ACCEPT}.
             */
            @Test
            @DisplayName(
                    "getActionForBlock() returns ACCEPT when the provided block number is the next expected one and previous action is ACCEPT")
            void testGetActionACCEPTPreviousAction() {
                // Initially, the next expected block number is 0L.
                // Call with next expected block number and previous action null in order to "start" streaming block 0L.
                final BlockAction firstCall = toTest.getActionForBlock(0L, null, publisherHandlerId);
                // Assert that the first call returns ACCEPT.
                assertThat(firstCall).isEqualTo(BlockAction.ACCEPT);
                // Call with next expected block number and previous action ACCEPT.
                final BlockAction secondCall = toTest.getActionForBlock(0L, firstCall, publisherHandlerId);
                // Assert that the second call also returns ACCEPT.
                assertThat(secondCall).isEqualTo(BlockAction.ACCEPT);
            }

            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#getActionForBlock(long, BlockAction, long)}
             * method returns {@link BlockAction#END_DUPLICATE} when the provided block
             * number is lower or equal to the latest known block number and
             * previous action is {@link BlockAction#ACCEPT}.
             */
            @Test
            @DisplayName(
                    "getActionForBlock() returns END_DUPLICATE when the provided block number is lower or equal to the latest known block number and previous action is ACCEPT")
            void testGetActionACCEPTPreviousActionDUPLICATE() {
                // Initially, the latest known block number is -1L.
                // Call with lower than latest known block number and previous action ACCEPT.
                final BlockAction actual = toTest.getActionForBlock(-2L, BlockAction.ACCEPT, publisherHandlerId);
                // Assert
                assertThat(actual).isEqualTo(BlockAction.END_DUPLICATE);
            }

            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#getActionForBlock(long, BlockAction, long)}
             * method returns {@link BlockAction#SKIP} when the provided block
             * number is both higher than the latest known block number and
             * lower than the current streaming block number, and previous
             * action is {@link BlockAction#ACCEPT}.
             */
            @Test
            @DisplayName(
                    "getActionForBlock() returns SKIP when the provided block number is both higher than the latest known block number and lower than the current streaming block number, and previous action is ACCEPT")
            void testGetActionACCEPTPreviousActionSKIP() {
                // Initially, the next expected block number is 0L.
                // Initially, the current streaming block number is same as next expected, i.e. 0L in this case.
                // For this test we need to actually send items to the publisher handler so that we can trigger
                // logic that will update the current streaming block number once we run messaging forwarder async.
                // First we need to build a block
                final long streamedBlockNumber = 0L;
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(1);
                // Now we build the request
                final BlockItemSetUnparsed itemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(block).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(itemSet)
                        .build();
                // We send the request to the publisher handler.
                // This will queue the messaging forwarder to run and update the current streaming block number.
                publisherHandler.onNext(request);
                // We run the queued messaging forwarder to update the current streaming block number.
                // We need to run the task async, because the loop (managed by config) is way too big to block on.
                // We will however wait for one second to ensure the task is run.
                threadPoolManager.executor().executeAsync(1_000L, false);
                // Call, now we expect to hit SKIP
                final BlockAction actual =
                        toTest.getActionForBlock(streamedBlockNumber, BlockAction.ACCEPT, publisherHandlerId);
                // Assert
                assertThat(actual).isEqualTo(BlockAction.SKIP);
            }

            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#getActionForBlock(long, BlockAction, long)}
             * method returns {@link BlockAction#END_ERROR} when the provided
             * block number equal to the next expected block number and previous
             * action is {@link BlockAction#ACCEPT}.
             */
            @Test
            @DisplayName(
                    "getActionForBlock() returns END_ERROR when the provided block number equal to the next expected block number and previous action is ACCEPT")
            void testGetActionACCEPTPreviousActionERROR() {
                // Initially, the next expected block number is 0L.
                // Call
                final BlockAction actual = toTest.getActionForBlock(0L, BlockAction.ACCEPT, publisherHandlerId);
                // Assert
                assertThat(actual).isEqualTo(BlockAction.END_ERROR);
            }

            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#getActionForBlock(long, BlockAction, long)}
             * method returns {@link BlockAction#END_BEHIND} when the provided
             * block number higher than the next expected block number and
             * previous action is {@link BlockAction#ACCEPT}.
             */
            @Test
            @DisplayName(
                    "getActionForBlock() returns END_BEHIND when the provided block number higher than the next expected block number and previous action is ACCEPT")
            void testGetActionACCEPTPreviousActionBEHIND() {
                // Initially, the next expected block number is 0L.
                // Call with higher than next expected block number and previous action ACCEPT.
                final BlockAction actual = toTest.getActionForBlock(1L, BlockAction.ACCEPT, publisherHandlerId);
                // Assert
                assertThat(actual).isEqualTo(BlockAction.END_BEHIND);
            }

            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#getActionForBlock(long, BlockAction, long)}
             * method returns {@link BlockAction#END_ERROR} when the provided
             * previous action is neither {@code null} nor {@link BlockAction#ACCEPT}.
             */
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

        /**
         * Test for {@link LiveStreamPublisherManager#getLatestBlockNumber()}.
         */
        @Nested
        @DisplayName("getLatestBlockNumber() Tests")
        class GetLatestBlockNumberTests {
            /**
             * This test aims to asser that the
             * {@link LiveStreamPublisherManager#getLatestBlockNumber()} will return
             * {@code -1L} when no blocks have been persisted yet.
             */
            @Test
            @DisplayName("getLatestBlockNumber() returns -1 when no blocks have been persisted yet")
            void testLatestBlockWhenNonePersisted() {
                // Call
                final long actual = toTest.getLatestBlockNumber();
                // Assert
                assertThat(actual).isEqualTo(-1L);
            }

            /**
             * This test aims to asser that the
             * {@link LiveStreamPublisherManager#getLatestBlockNumber()} will return
             * the latest block number that has been persisted.
             */
            @Test
            @DisplayName("getLatestBlockNumber() returns latest persisted block number")
            void testLatestBlockNumber() {
                // Assert that the latest block number is -1L before we persist any blocks.
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(-1L);
                // Generate block with number 0 and send it to the historical block facility.
                final BlockItemUnparsed[] block0 = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(1);
                // The below call will send a persisted notification which will be picked up by the
                // instance under test.
                final long expectedLatestBlockNumber = 0L;
                historicalBlockFacility.handleBlockItemsReceived(
                        new BlockItems(List.of(block0), expectedLatestBlockNumber));
                // Call
                final long actual = toTest.getLatestBlockNumber();
                // Assert that the latest block number is now 0.
                assertThat(actual).isEqualTo(expectedLatestBlockNumber);
            }

            /**
             * This test aims to asser that the
             * {@link LiveStreamPublisherManager#getLatestBlockNumber()} will return
             * the latest block number that has been persisted during object
             * construction.
             */
            @Test
            @DisplayName("getLatestBlockNumber() returns latest persisted block number during construction")
            void testLatestBlockNumberDuringConstruction() {
                final SimpleInMemoryHistoricalBlockFacility localHistoricalBlockFacility =
                        new SimpleInMemoryHistoricalBlockFacility();
                final BlockItemUnparsed[] block0 = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(1);
                final long expectedLatestBlockNumber = 0L;
                // Persist the block in the local historical block facility, we do not need to send a notification.
                localHistoricalBlockFacility.handleBlockItemsReceived(
                        new BlockItems(List.of(block0), expectedLatestBlockNumber), false);
                // Construct a new LiveStreamPublisherManager with the local historical block facility.
                final LiveStreamPublisherManager localToTest = new LiveStreamPublisherManager(
                        generateContext(localHistoricalBlockFacility), generateManagerMetrics());
                // After construction, the latest block number should be the one we just persisted.
                // Call
                final long actual = localToTest.getLatestBlockNumber();
                // Assert that the latest block number is now 0.
                assertThat(actual).isEqualTo(expectedLatestBlockNumber);
            }
        }

        /**
         * Tests for {@link LiveStreamPublisherManager#closeBlock(long)}.
         *
         * Validate that the publisher manager correctly closes blocks.<br/>
         * This inner class verifies that blocks are closed, metrics are correctly updated
         * operation order is respected.
         * <p>
         * Specific items include
         * <ol>
         *   <li>Metrics are updated after batches are forwarded</li>
         *   <li>All pending batches are forwarded to messaging</li>
         *   <li>The forwarder task is correctly started or restarted, if necessary</li>
         *   <li>Metrics are not updated in the middle of sending data to messaging</li>
         * </ol>
         */
        @Nested
        @DisplayName("closeBlock() Tests")
        @Timeout(value = 10, unit = TimeUnit.SECONDS)
        class CloseBlockTests {

            /**
             * Helper to wait for the forwarder to finish, up to {@code timeoutMs}.
             */
            private void awaitBatchesIncrement(final long before, final long timeoutMs) throws InterruptedException {
                // Compute a deadline (wall-clock millis) after which we give up waiting.
                final long deadline = System.currentTimeMillis() + timeoutMs;
                // Busy-wait in short sleeps until the batches metric increases beyond the 'before' baseline.
                while (System.currentTimeMillis() < deadline) {
                    // If the forwarder has completed at least one batch, the metric will be greater than baseline.
                    if (managerMetrics.blockBatchesMessaged().get() > before) return;
                    // Sleep briefly to avoid a hot spin while still reacting quickly when the metric changes.
                    Thread.sleep(10L);
                }
                // If we reach here, the timeout elapsed without observing an increment; let the caller assert as
                // needed.
            }

            /**
             * Verifies that completed blocks update immediate and post-forwarder metrics and forward payloads.
             */
            @Test
            @DisplayName("completed block updates metrics immediately and forwards after drain")
            void testCompletedBlockUpdatesMetricsAndForwards() throws InterruptedException {
                // Use block number 0 for this scenario.
                final long blockNumber = 0L;

                // Build at least one synthetic block item for the block we will close.
                final BlockItemUnparsed[] items =
                        SimpleTestBlockItemBuilder.createSimpleBlockUnparsedWithNumber(blockNumber);
                // Header-first sanity check
                assertThat(items[0].hasBlockHeader())
                        .as("first item must be a BlockHeader for block " + blockNumber)
                        .isTrue();
                // Wrap items into a request payload for the publisher.
                final PublishStreamRequestUnparsed req = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(BlockItemSetUnparsed.newBuilder()
                                .blockItems(items)
                                .build())
                        .build();

                final BlockAction startAction = toTest.getActionForBlock(blockNumber, null, publisherHandlerId);
                assertThat(startAction).isEqualTo(BlockAction.ACCEPT);
                // Enqueue the request into the handler (this may also schedule the forwarder in production code).
                publisherHandler.onNext(req);
                // Mark the block as eligible to be closed (end-of-items for this block).
                endThisBlock(publisherHandler, blockNumber);

                // Capture the starting value for the async batches counter.
                final long beforeBatches = managerMetrics.blockBatchesMessaged().get();
                // Capture the starting value for the immediate-close counter.
                final long beforeClosed = managerMetrics.blocksClosedComplete().get();

                // Sanity check: no messages have been pushed yet before we trigger close.
                assertThat(messagingFacility.getSentBlockItems()).isEmpty();
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(blockNumber - 1);

                // Close the block; this should increment the immediate metric synchronously.
                toTest.closeBlock(blockNumber);

                // Immediate metric should reflect one close; the messaging facility remains empty until the forwarder
                // runs.
                assertThat(managerMetrics.blocksClosedComplete().get()).isEqualTo(beforeClosed + 1);
                assertThat(messagingFacility.getSentBlockItems()).isEmpty();

                // Execute the queued task.
                threadPoolManager.executor().executeAsync(1_000L, false);
                // Wait (up to 3s) for the batches metric to increase beyond its baseline.
                awaitBatchesIncrement(beforeBatches, 3_000L);

                // Post-forwarder: both onNext() and closeBlock() may schedule; expect two batches produced.
                assertThat(managerMetrics.blocksClosedComplete().get()).isEqualTo(beforeBatches + 2);
                assertThat(managerMetrics.currentPublisherCount().get()).isEqualTo(beforeBatches + 2);
                // The in-memory messaging facility should now have reset the block number to -1.
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(blockNumber - 1);
            }

            /**
             * Verifies metric gating — batches only increment after the forwarder completes.
             */
            @Test
            @DisplayName("batches increment only after forwarder completes (gating)")
            void testBatchesIncrementOnlyAfterForwarderCompletes() throws InterruptedException {
                // Baseline the async batches counter.
                final long beforeBatches = managerMetrics.blockBatchesMessaged().get();
                // Use a distinct block number for isolation from other tests.
                final long blockNumber = 100L;

                // Build items for the same block that we will close.
                final BlockItemUnparsed[] items = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                        (int) blockNumber, (int) blockNumber);
                // Wrap them into a publish request.
                final PublishStreamRequestUnparsed req = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(BlockItemSetUnparsed.newBuilder()
                                .blockItems(items)
                                .build())
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
                assertThat(managerMetrics.blocksClosedComplete().get()).isEqualTo(beforeBatches + 2);
                assertThat(managerMetrics.currentPublisherCount().get()).isEqualTo(beforeBatches + 1);
                // The in-memory messaging facility should now have reset the block number to -1.
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(-1);
            }

            /**
             * Verifies the forwarder restarts after completion — two runs yield four batches total.
             */
            @Test
            @DisplayName("restarts forwarder after completion (two runs → four batches)")
            void testRestartForwarderAfterCompletion() throws InterruptedException {
                // ===== Run #1 =====
                // Use a unique block number for the first run.
                final long b0 = 2L;
                // Build items for block #b0.
                final BlockItemUnparsed[] items0 =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed((int) b0, (int) b0);
                // Wrap into a request.
                final PublishStreamRequestUnparsed req0 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(BlockItemSetUnparsed.newBuilder()
                                .blockItems(items0)
                                .build())
                        .build();

                // Enqueue to handler.
                publisherHandler.onNext(req0);
                // Mark block b0 as ended.
                endThisBlock(publisherHandler, b0);
                // Baseline both async batches and immediate close counters.
                final long beforeBatches = managerMetrics.blockBatchesMessaged().get();
                final long beforeClosed = managerMetrics.blocksClosedComplete().get();
                // Close the block (immediate metric should +1).
                toTest.closeBlock(b0);
                // Verify immediate close counter progressed by exactly one.
                assertThat(managerMetrics.blocksClosedComplete().get()).isEqualTo(beforeClosed + 1);
                // Execute the queued tasks; the test pool throws if the queue is empty, enforcing correct sequencing.
                threadPoolManager.executor().executeAsync(1_000L, false);
                // Wait until batches surpass baseline.
                awaitBatchesIncrement(beforeBatches, 3_000L);
                // After completion, we expect two batches (onNext + closeBlock scheduling).
                assertThat(managerMetrics.blocksClosedComplete().get()).isEqualTo(beforeBatches + 2);
                // The in-memory messaging facility should now have reset the block number to -1.
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(-1);

                // ===== Run #2 =====
                // Use another unique block number for isolation.
                final long b1 = 3L;
                // Build items for block #b1.
                final BlockItemUnparsed[] items1 =
                        SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed((int) b1, (int) b1);
                // Wrap into a request.
                final PublishStreamRequestUnparsed req1 = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(BlockItemSetUnparsed.newBuilder()
                                .blockItems(items1)
                                .build())
                        .build();

                // Enqueue to handler.
                publisherHandler.onNext(req1);
                // Mark block b1 as ended.
                endThisBlock(publisherHandler, b1);
                // Close the block
                toTest.closeBlock(b1);
                assertThat(managerMetrics.blocksClosedComplete().get()).isEqualTo(beforeClosed + 3);

                // Wait until batches surpass the +2 baseline from the first run.
                awaitBatchesIncrement(beforeBatches + 2, 3_000L);

                // After the second completion, we expect four batches total (two per run).
                // After completion, we expect two batches (onNext + closeBlock scheduling).
                assertThat(managerMetrics.blocksClosedComplete().get()).isEqualTo(beforeBatches + 4);
                // The in-memory messaging facility should now have reset the block number to -1.
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(-1);
            }

            /**
             * Verifies idempotency — repeated closes while forwarder is active don’t double-count.
             */
            @Test
            @DisplayName("no batch/count updates while forwarder active (idempotent closes)")
            void testNoMetricUpdatesWhileForwarderActive() throws InterruptedException {
                // Use a unique block number for this scenario.
                final long blockNumber = 4L;

                // Build items for this block.
                final BlockItemUnparsed[] items = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(
                        (int) blockNumber, (int) blockNumber);
                // Wrap into a request.
                final PublishStreamRequestUnparsed req = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(BlockItemSetUnparsed.newBuilder()
                                .blockItems(items)
                                .build())
                        .build();

                // Enqueue to handler.
                publisherHandler.onNext(req);
                // Mark this block as ended so it becomes eligible for closure.
                endThisBlock(publisherHandler, blockNumber);

                // Baseline metrics.
                final long beforeBatches = managerMetrics.blockBatchesMessaged().get();

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
                assertThat(managerMetrics.blocksClosedComplete().get()).isEqualTo(beforeBatches + 4);
                // The in-memory messaging facility should now have reset the block number to -1.
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(-1);
            }
        }

        /**
         * Tests for {@link LiveStreamPublisherManager#handleVerification(VerificationNotification)}.
         */
        @Nested
        @DisplayName("handleVerification() Tests")
        class HandleVerificationTests {
            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#handleVerification(VerificationNotification)}
             * does nothing when the notification states that the block has passed verification.
             * We expect that no responses are sent.
             */
            @Test
            @DisplayName("handleVerification() does nothing when block has passed verification, no responses sent")
            void testHandleVerificationPassed() {
                // We need to send a request via the publisher handler first,
                // This will properly update the internal state of the manager
                // so we can assert correctly. We aim to increment the next
                // unstreamed block number to 1L so we have a gap between
                // latest persisted (which should be -1L) and next unstreamed.
                // This is an expected condition during normal operation.
                final long streamedBlockNumber = 0L;
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(1);
                // Now we build the request
                final BlockItemSetUnparsed itemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(block).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(itemSet)
                        .build();
                // We send the request to the publisher handler.
                // This will update the next unstreamed block number to 1L as
                // soon as we start streaming, i.e. a handler has queried the
                // manager for a block action for block 0L and at that point it
                // was the next expected block.
                publisherHandler.onNext(request);
                endThisBlock(publisherHandler, streamedBlockNumber);
                // Build a verification notification with passed verification.
                // Source must be publisher.
                final VerificationNotification notification =
                        new VerificationNotification(true, streamedBlockNumber, null, null, BlockSource.PUBLISHER);
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

            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#handleVerification(VerificationNotification)}
             * does nothing when the failed block's number is equal the
             * latest know block in the manager.
             * We expect that no responses are sent.
             */
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
                final long streamedBlockNumber = 0L;
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(1);
                // Now we build the request
                final BlockItemSetUnparsed itemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(block).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(itemSet)
                        .build();
                // We send the request to the publisher handler.
                // This will update the next unstreamed block number to 1L as
                // soon as we start streaming, i.e. a handler has queried the
                // manager for a block action for block 0L and at that point it
                // was the next expected block.
                publisherHandler.onNext(request);
                endThisBlock(publisherHandler, streamedBlockNumber);
                // Now we need to send a PersistedNotification, so that the
                // latest known block number will be updated to 0L.
                final PersistedNotification persistedNotification =
                        new PersistedNotification(streamedBlockNumber, true, 0, BlockSource.PUBLISHER);
                // Send the persisted notification to the manager.
                toTest.handlePersisted(persistedNotification);
                // Assert that the latest known block number is now 0L.
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(streamedBlockNumber);
                // Clear the pipeline because an acknowledgement response has been sent due to the
                // persisted notification.
                responsePipeline.clear();
                // Build a verification notification with block number equal to the latest known.
                // Source must be publisher.
                final VerificationNotification notification =
                        new VerificationNotification(false, streamedBlockNumber, null, null, BlockSource.PUBLISHER);
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

            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#handleVerification(VerificationNotification)}
             * does nothing when the failed block's number is lower than the
             * latest know block in the manager.
             * We expect that no responses are sent.
             */
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
                final long streamedBlockNumber = 0L;
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(1);
                // Now we build the request
                final BlockItemSetUnparsed itemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(block).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(itemSet)
                        .build();
                // We send the request to the publisher handler.
                // This will update the next unstreamed block number to 1L as
                // soon as we start streaming, i.e. a handler has queried the
                // manager for a block action for block 0L and at that point it
                // was the next expected block.
                publisherHandler.onNext(request);
                endThisBlock(publisherHandler, streamedBlockNumber);
                // We need to send a PersistedNotification first, so that the latest known block number will be updated
                // to 0L.
                final PersistedNotification persistedNotification =
                        new PersistedNotification(streamedBlockNumber, true, 0, BlockSource.PUBLISHER);
                // Send the persisted notification to the manager.
                toTest.handlePersisted(persistedNotification);
                // Assert that the latest known block number is now 0L.
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(streamedBlockNumber);
                // Clear the pipeline because an acknowledgement response has been sent due to the
                // persisted notification.
                responsePipeline.clear();
                // Build a verification notification with block number lower than the latest known.
                // Source must be publisher.
                final VerificationNotification notification = new VerificationNotification(
                        false, streamedBlockNumber - 1L, null, null, BlockSource.PUBLISHER);
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

            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#handleVerification(VerificationNotification)}
             * does nothing when the failed block's number is equal the
             * next unstreamed block in the manager.
             * We expect that no responses are sent.
             */
            @Test
            @DisplayName(
                    "handleVerification() does nothing when block number of failed block is equal to the next unstreamed")
            void testHandleVerificationEqualToNext() {
                // Initially, the next unstreamed block number is 0L.
                final long streamedBlockNumber = 0L;
                final VerificationNotification notification =
                        new VerificationNotification(false, streamedBlockNumber, null, null, BlockSource.PUBLISHER);
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

            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#handleVerification(VerificationNotification)}
             * does nothing when the failed block's number is higher than the
             * next unstreamed block in the manager.
             * We expect that no responses are sent.
             */
            @Test
            @DisplayName(
                    "handleVerification() does nothing when block number of failed block is higher than the next unstreamed")
            void testHandleVerificationHigherThanNext() {
                // Initially, the next unstreamed block number is 0L.
                final long streamedBlockNumber = 1L;
                final VerificationNotification notification =
                        new VerificationNotification(false, streamedBlockNumber, null, null, BlockSource.PUBLISHER);
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

            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#handleVerification(VerificationNotification)}
             * will produce a {@link PublishStreamResponse.EndOfStream} response with
             * {@link Code#BAD_BLOCK_PROOF} to the response pipeline of the handler
             * that supplied the block with invalid proof.
             */
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
                final long streamedBlockNumber = 0L;
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(1);
                // Now we build the request
                final BlockItemSetUnparsed itemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(block).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(itemSet)
                        .build();
                // We send the request to the publisher handler.
                // This will update the next unstreamed block number to 1L as
                // soon as we start streaming, i.e. a handler has queried the
                // manager for a block action for block 0L and at that point it
                // was the next expected block.
                publisherHandler.onNext(request);
                endThisBlock(publisherHandler, streamedBlockNumber);
                // Now, the publisher has sent the targeted block with broken proof.
                // We can now build a verification notification with failed verification.
                final VerificationNotification notification =
                        new VerificationNotification(false, streamedBlockNumber, null, null, BlockSource.PUBLISHER);
                // Call
                toTest.handleVerification(notification);
                // Assert that the response pipeline has received a BAD_BLOCK_PROOF response, because the
                // publisher we used has sent a block with invalid proof and handler shutdown (onComplete called).
                assertThat(responsePipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.END_STREAM, responseKindExtractor)
                        .returns(Code.BAD_BLOCK_PROOF, endStreamResponseCodeExtractor)
                        // below block number in the response is the latest known, -1L because none are stored
                        .returns(-1L, endStreamBlockNumberExtractor);
                assertThat(responsePipeline.getOnCompleteCalls().get()).isEqualTo(1);
                assertThat(sharedHandlerMetrics.endOfStreamsSent().get()).isEqualTo(1);
                // Assert no other responses sent
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#handleVerification(VerificationNotification)}
             * will produce a {@link PublishStreamResponse.ResendBlock} response to the response
             * pipelines of all handlers that did not supplied the block with
             * invalid proof that failed verification.
             */
            @Test
            @DisplayName(
                    "handleVerification() RESEND response is sent by all handlers that did not supply the block with invalid proof that failed verification")
            void testHandleVerificationResend() {
                // We need to send a request via the publisher handler first,
                // This will properly update the internal state of the manager
                // so we can assert correctly. We aim to increment the next
                // unstreamed block number to 1L so we have a gap between
                // latest persisted (which should be -1L) and next unstreamed.
                // This is an expected condition during normal operation.
                final long streamedBlockNumber = 0L;
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(1);
                // Now we build the request
                final BlockItemSetUnparsed itemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(block).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(itemSet)
                        .build();
                // We send the request to the second publisher handler.
                // This will update the next unstreamed block number to 1L as
                // soon as we start streaming, i.e. a handler has queried the
                // manager for a block action for block 0L and at that point it
                // was the next expected block.
                publisherHandler2.onNext(request);
                endThisBlock(publisherHandler2, streamedBlockNumber);
                // Build a verification notification with failed verification.
                final VerificationNotification notification =
                        new VerificationNotification(false, streamedBlockNumber, null, null, BlockSource.PUBLISHER);
                // Call
                toTest.handleVerification(notification);
                // Assert that the response pipeline has received a RESEND response, because the
                // publisher we used has not sent the block with invalid proof.
                assertThat(responsePipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.RESEND_BLOCK, responseKindExtractor)
                        // below block number in the response is the latest known +1, i.e. 0L because none are stored
                        // which is -1L + 1L = 0L
                        .returns(0L, resendBlockNumberExtractor);
                assertThat(sharedHandlerMetrics.blockResendsSent().get()).isEqualTo(1);
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

            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#handleVerification(VerificationNotification)}
             * will does not stop the normal operation of the publisher manager
             * and subsequent items received by handlers will be processed
             * accordingly.
             */
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
                final long streamedBlockNumber = 0L;
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(1);
                // Now we build the request
                final BlockItemSetUnparsed itemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(block).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(itemSet)
                        .build();
                // We send the request to the second publisher handler.
                // This will update the next unstreamed block number to 1L as
                // soon as we start streaming, i.e. a handler has queried the
                // manager for a block action for block 0L and at that point it
                // was the next expected block.
                publisherHandler2.onNext(request);
                endThisBlock(publisherHandler2, streamedBlockNumber);
                // Then, we need to simulate that the publisher has sent a block with invalid proof, i.e. call
                // handleVerification with failed verification.
                // Build a verification notification with failed verification.
                final VerificationNotification notification =
                        new VerificationNotification(false, streamedBlockNumber, null, null, BlockSource.PUBLISHER);
                // Call
                toTest.handleVerification(notification);
                // Assert that the response pipeline has received a RESEND response, because the
                // publisher we check for here used has not sent the block with invalid proof.
                assertThat(responsePipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.RESEND_BLOCK, responseKindExtractor)
                        // below block number in the response is latest known +1, i.e. 0L because none are stored
                        // which is -1L + 1L = 0L
                        .returns(0L, resendBlockNumberExtractor);
                assertThat(sharedHandlerMetrics.blockResendsSent().get()).isEqualTo(1);
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
                assertThat(sharedHandlerMetrics.endOfStreamsSent().get()).isEqualTo(1);
                // Assert no other responses sent
                assertThat(responsePipeline2.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline2.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline2.getClientEndStreamCalls().get()).isEqualTo(0);
                // Now, we clear the first handler's pipeline
                responsePipeline.clear();
                // Before we send the request (we can reuse from above), we assert that the messaging
                // facility has no items received.
                assertThat(messagingFacility.getSentBlockItems()).isNotNull().isEmpty();
                // We send the request to the publisher handler.
                publisherHandler.onNext(request);
                endThisBlock(publisherHandler, streamedBlockNumber);
                // We run the queued messaging forwarder to update the current streaming block number.
                // We need to run the task async, because the loop (managed by config) is way too big to block on.
                // We will however wait for one second to ensure the task is run.
                threadPoolManager.executor().executeAsync(1_000L, false);
                // Assert that items were propagated to the publisher handler.
                assertThat(messagingFacility.getSentBlockItems())
                        .isNotNull()
                        .isNotEmpty()
                        .hasSize(1);
                assertThat(messagingFacility.getSentBlockItems().getFirst().blockItems())
                        .isNotNull()
                        .isNotEmpty()
                        .hasSize(block.length)
                        .containsExactly(block);
            }
        }

        /**
         * Tests for {@link LiveStreamPublisherManager#handlePersisted(PersistedNotification)}.
         */
        @Nested
        @DisplayName("handlePersisted() Tests")
        class HandlePersistedTests {
            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#handlePersisted(PersistedNotification)}
             * will send acknowledgement to registered publisher handlers
             * with the latest block number, i.e.
             * {@link PersistedNotification#blockNumber()}.
             */
            @Test
            @DisplayName("handlePersisted() sends acknowledgement with latest block number to all registered handlers")
            void testHandlePersistedValidNotification() {
                // As a precondition, assert that the responses pipeline is empty (nothing has been sent yet).
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                // Build the notification with end block number 10L.
                final long expectedLatestBlockNumber = 10L;
                final PersistedNotification notification =
                        new PersistedNotification(10L, true, 0, BlockSource.PUBLISHER);
                // Call
                toTest.handlePersisted(notification);
                // Assert that the response pipeline has received a response with the expected latest block number.
                // We have one handler, so we expect one response.
                assertThat(responsePipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                        .returns(expectedLatestBlockNumber, acknowledgementBlockNumberExtractor);
                assertThat(managerMetrics.latestBlockNumberAcknowledged().get()).isEqualTo(expectedLatestBlockNumber);
                // Assert no other responses sent
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(responsePipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#handlePersisted(PersistedNotification)}
             * will set the latest known block number to the
             * {@link PersistedNotification#blockNumber()}.
             */
            @Test
            @DisplayName("handlePersisted() sets latest known block number to notification's endBlockNumber")
            void testHandlePersistedSetsLatestKnownBlockNumber() {
                // As a precondition, assert that the latest known block number is -1L (nothing has been persisted yet).
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(-1L);
                // Build the notification with end block number 10L.
                final long expectedLatestBlockNumber = 10L;
                final PersistedNotification notification =
                        new PersistedNotification(10L, true, 0, BlockSource.PUBLISHER);
                // Call
                toTest.handlePersisted(notification);
                assertThat(managerMetrics.latestBlockNumberAcknowledged().get()).isEqualTo(expectedLatestBlockNumber);
                // Assert that the latest known block number is now set to the notification's end block number.
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(expectedLatestBlockNumber);
            }

            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#handlePersisted(PersistedNotification)}
             * will not send acknowledgement to registered publisher handlers
             * when the persistence has failed, i.e.
             * {@link PersistedNotification#succeeded()} is false.
             */
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
                // Assert that the latest known block number is still -1L, it was not updated
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(expectedLatestPersistedFromManager);
                assertThat(responsePipeline.getOnCompleteCalls().get()).isEqualTo(1);

                // Assert no other responses sent
                assertThat(responsePipeline.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#handlePersisted(PersistedNotification)}
             * will not send acknowledgement to registered publisher handlers
             * when the notification is null.
             */
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

        /**
         * Tests for {@link LiveStreamPublisherManager#handlerIsEnding(long, long)}.
         */
        @Nested
        @DisplayName("handleIsEnding() Tests")
        class HandlerIsEndingTests {
            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#handlerIsEnding(long, long)}
             * will correctly handle an end stream request when the handler
             * has completed it's current streaming block.
             */
            @ParameterizedTest()
            @EnumSource(EndStream.Code.class)
            @DisplayName("Test handleIsEnding() with complete block")
            void testHandlerIsEndingWithCompleteBlock(final EndStream.Code code) {
                // First, we build a valid request and send it to the publisher.
                // This will query for block action which will update the state
                // of the manager to have a next unstreamed block number of 1L.
                final long streamedBlockNumber = 0L;
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(1);
                // Now we build the request
                final BlockItemSetUnparsed itemSet =
                        BlockItemSetUnparsed.newBuilder().blockItems(block).build();
                final PublishStreamRequestUnparsed request = PublishStreamRequestUnparsed.newBuilder()
                        .blockItems(itemSet)
                        .build();
                // We send the request to the publisher handler.
                publisherHandler.onNext(request);
                endThisBlock(publisherHandler, streamedBlockNumber);
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
                assertThat(sharedHandlerMetrics.endStreamsReceived().get()).isEqualTo(1);
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
                endThisBlock(publisherHandler, streamedBlockNumber);
                assertThat(responsePipeline2.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.SKIP_BLOCK, responseKindExtractor)
                        .returns(streamedBlockNumber, skipBlockNumberExtractor);
                assertThat(sharedHandlerMetrics.blockSkipsSent().get()).isEqualTo(1);
                // Assert no other responses sent
                assertThat(responsePipeline2.getOnErrorCalls()).isEmpty();
                assertThat(responsePipeline2.getOnSubscriptionCalls()).isEmpty();
                assertThat(responsePipeline2.getOnCompleteCalls().get()).isEqualTo(0);
                assertThat(responsePipeline2.getClientEndStreamCalls().get()).isEqualTo(0);
            }

            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#handlerIsEnding(long, long)}
             * will correctly handle an end stream request when the handler
             * has not completed it's current streaming block.
             */
            @ParameterizedTest()
            @EnumSource(EndStream.Code.class)
            @DisplayName("Test handleIsEnding() with incomplete block")
            void testHandlerIsEndingWithIncompleteBlock(final EndStream.Code code) {
                // First, we build a valid request and send it to the publisher.
                // This will query for block action which will update the state
                // of the manager to have a next unstreamed block number of 1L.
                final long streamedBlockNumber = 0L;
                final BlockItemUnparsed[] block = SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocksUnparsed(1);
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
                        .earliestBlockNumber(streamedBlockNumber)
                        .latestBlockNumber(streamedBlockNumber)
                        .build();
                // Build a PublishStreamRequest with the EndStream
                final PublishStreamRequestUnparsed endStreamRequest = PublishStreamRequestUnparsed.newBuilder()
                        .endStream(endStream)
                        .build();
                // Now we send the end stream request to the publisher handler.
                publisherHandler.onNext(endStreamRequest);
                // Now we must assert that the publisher has shutdown
                assertThat(responsePipeline.getOnCompleteCalls().get()).isEqualTo(1);
                assertThat(sharedHandlerMetrics.endStreamsReceived().get()).isEqualTo(1);
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
                endThisBlock(publisherHandler, streamedBlockNumber);
                // We run the queued messaging forwarder to update the current streaming block number.
                // We need to run the task async, because the loop (managed by config) is way too big to block on.
                // We will however wait for one second to ensure the task is run.
                threadPoolManager.executor().executeAsync(1_000L, false);
                // Assert that the second request has been accepted and the block items were sent.
                assertThat(messagingFacility.getSentBlockItems().getFirst().blockItems())
                        .isNotNull()
                        .isNotEmpty()
                        .hasSize(block.length)
                        .containsExactly(block);
            }
        }
    }

    /**
     * This method generates a {@link BlockNodeContext} instance with default
     * facilities that can be used in tests.
     */
    private BlockNodeContext generateContext() {
        final HistoricalBlockFacility historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
        return generateContext(historicalBlockFacility);
    }

    /**
     * This method generates a {@link BlockNodeContext} instance with default
     * facilities that can be used in tests.
     */
    private BlockNodeContext generateContext(final HistoricalBlockFacility historicalBlockFacility) {
        final ThreadPoolManager threadPoolManager =
                new TestThreadPoolManager<>(new BlockingExecutor(new LinkedBlockingQueue<>()));
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
                threadPoolManager);
    }

    private static Configuration createTestConfiguration() {
        return TestUtils.createTestConfiguration()
                .withConfigDataType(PublisherConfig.class)
                .build();
    }

    /**
     * This method generates a {@link MetricsHolder} instance with default
     * metrics that can be used in tests.
     */
    private MetricsHolder generateManagerMetrics() {
        return MetricsHolder.createMetrics(TestUtils.createMetrics());
    }

    /**
     * Creates a new {@link PublisherHandler.MetricsHolder} with default counters for testing.
     * These counters could be queried to verify the metrics' states.
     */
    private PublisherHandler.MetricsHolder generateHandlerMetrics() {
        return PublisherHandler.MetricsHolder.createMetrics(TestUtils.createMetrics());
    }
}
