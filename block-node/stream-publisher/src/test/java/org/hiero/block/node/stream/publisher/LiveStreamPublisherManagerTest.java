// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.hedera.hapi.block.stream.BlockProof;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import com.swirlds.metrics.api.Counter.Config;
import com.swirlds.metrics.api.LongGauge;
import com.swirlds.metrics.api.Metrics;
import com.swirlds.metrics.impl.DefaultCounter;
import com.swirlds.metrics.impl.DefaultLongGauge;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.PublishStreamResponse.ResponseOneOfType;
import org.hiero.block.internal.BlockItemSetUnparsed;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.PublishStreamRequestUnparsed;
import org.hiero.block.node.app.fixtures.async.BlockingExecutor;
import org.hiero.block.node.app.fixtures.async.TestThreadPoolManager;
import org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestBlockMessagingFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.ServiceLoaderFunction;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.hiero.block.node.spi.health.HealthFacility;
import org.hiero.block.node.spi.historicalblocks.HistoricalBlockFacility;
import org.hiero.block.node.spi.threading.ThreadPoolManager;
import org.hiero.block.node.stream.publisher.LiveStreamPublisherManager.MetricsHolder;
import org.hiero.block.node.stream.publisher.StreamPublisherManager.BlockAction;
import org.hiero.block.node.stream.publisher.fixtures.TestResponsePipeline;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

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
        /** The context to use when testing */
        private BlockNodeContext context;
        /** The response pipeline to use when testing */
        private TestResponsePipeline responsePipeline;
        /** The publisher handler to use when testing */
        private PublisherHandler publisherHandler;
        /** The ID of the publisher handler, used to identify it in the manager */
        private long publisherHandlerId;
        /** The instance under test */
        private LiveStreamPublisherManager toTest;

        // EXTRACTORS
        private final Function<PublishStreamResponse, ResponseOneOfType> responseKindExtractor =
                response -> response.response().kind();
        private final Function<PublishStreamResponse, Long> acknowledgementBlockNumberExtractor =
                response -> Objects.requireNonNull(response.acknowledgement()).blockNumber();

        /**
         * Environment setup called before each test.
         */
        @BeforeEach
        void setup() {
            // Initialize the historical block facility and the context.
            historicalBlockFacility = new SimpleInMemoryHistoricalBlockFacility();
            threadPoolManager = new TestThreadPoolManager<>(new BlockingExecutor(new LinkedBlockingQueue<>()));
            final BlockNodeContext context = generateContext(historicalBlockFacility, threadPoolManager);
            // Initialize the historical block facility with the context.
            historicalBlockFacility.init(context, null);
            // Create the metrics hodler for the manager.
            final MetricsHolder managerMetrics = generateManagerMetrics();
            // Create the LiveStreamPublisherManager instance to test.
            toTest = new LiveStreamPublisherManager(context, managerMetrics);
            // We need to explicitly register the manager as a notification handler
            // The manager does not register itself.
            context.blockMessaging()
                    .registerBlockNotificationHandler(toTest, false, LiveStreamPublisherManager.class.getSimpleName());
            // Initialize the response pipeline and the publisher handler.
            final PublisherHandler.MetricsHolder handlerMetrics = generateHandlerMetrics();
            // Create a response pipeline to handle the responses from the publisher handler.
            responsePipeline = new TestResponsePipeline();
            // Create a publisher handler using the manager's addHandler method.
            publisherHandler = toTest.addHandler(responsePipeline, handlerMetrics);
            publisherHandlerId = 0L; // This should be set by the addHandler method, first call will use id 0L.
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
            @Test
            @DisplayName(
                    "getActionForBlock() returns END_DUPLICATE when the provided block number is lower or equal to the latest known block number and previous action is NULL")
            void testGetActionNullPreviousActionDUPLICATE() {
                // Initially, the latest known block number is -1L.
                // Call with lower than latest known block number.
                final BlockAction actual = toTest.getActionForBlock(-2L, null, publisherHandlerId);
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
         * Tests for {@link LiveStreamPublisherManager#closeBlock(BlockProof, long)}.
         */
        @Nested
        @DisplayName("closeBlock() Tests")
        class CloseBlockTests {
            // @todo(1416) cannot test this yet, because the method is not implemented.
        }

        /**
         * Tests for {@link LiveStreamPublisherManager#handleVerification(VerificationNotification)}.
         */
        @Nested
        @DisplayName("handleVerification() Tests")
        class HandleVerificationTests {
            // @todo(1422) cannot test this yet, because the method is not implemented.
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
             * {@link PersistedNotification#endBlockNumber()}.
             */
            @Test
            @DisplayName("handlePersisted() sends acknowledgement with latest block number to all registered handlers")
            void testHandlePersistedValidNotification() {
                // As a precondition, assert that the responses pipeline is empty (nothing has been sent yet).
                assertThat(responsePipeline.getOnNextCalls()).isEmpty();
                // Build the notification with end block number 10L.
                final long expectedLatestBlockNumber = 10L;
                final PersistedNotification notification =
                        new PersistedNotification(0L, expectedLatestBlockNumber, 0, BlockSource.PUBLISHER);
                // Call
                toTest.handlePersisted(notification);
                // Assert that the response pipeline has received a response with the expected latest block number.
                // We have one handler, so we expect one response.
                assertThat(responsePipeline.getOnNextCalls())
                        .hasSize(1)
                        .first()
                        .returns(ResponseOneOfType.ACKNOWLEDGEMENT, responseKindExtractor)
                        .returns(expectedLatestBlockNumber, acknowledgementBlockNumberExtractor);
            }

            /**
             * This test aims to assert that the
             * {@link LiveStreamPublisherManager#handlePersisted(PersistedNotification)}
             * will set the latest known block number to the
             * {@link PersistedNotification#endBlockNumber()}.
             */
            @Test
            @DisplayName("handlePersisted() sets latest known block number to notification's endBlockNumber")
            void testHandlePersistedSetsLatestKnownBlockNumber() {
                // As a precondition, assert that the latest known block number is -1L (nothing has been persisted yet).
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(-1L);
                // Build the notification with end block number 10L.
                final long expectedLatestBlockNumber = 10L;
                final PersistedNotification notification =
                        new PersistedNotification(0L, expectedLatestBlockNumber, 0, BlockSource.PUBLISHER);
                // Call
                toTest.handlePersisted(notification);
                // Assert that the latest known block number is now set to the notification's end block number.
                assertThat(toTest.getLatestBlockNumber()).isEqualTo(expectedLatestBlockNumber);
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
        final TestThreadPoolManager<BlockingExecutor> threadPoolManager =
                new TestThreadPoolManager<>(new BlockingExecutor(new LinkedBlockingQueue<>()));
        return generateContext(historicalBlockFacility, threadPoolManager);
    }

    /**
     * This method generates a {@link BlockNodeContext} instance with default
     * facilities that can be used in tests.
     */
    @SuppressWarnings("all")
    private BlockNodeContext generateContext(
            final HistoricalBlockFacility historicalBlockFacility, final ThreadPoolManager threadPoolManager) {
        final Configuration configuration = ConfigurationBuilder.create()
                .withConfigDataType(PublisherConfig.class)
                .build();
        final Metrics metrics = null;
        final HealthFacility serverHealth = null;
        final TestBlockMessagingFacility blockMessagingFacility = new TestBlockMessagingFacility();
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

    /**
     * This method generates a {@link MetricsHolder} instance with default
     * metrics that can be used in tests.
     */
    private MetricsHolder generateManagerMetrics() {
        return new MetricsHolder(
                new DefaultCounter(new Config("category", "name")),
                new DefaultLongGauge(new LongGauge.Config("category", "name")),
                new DefaultLongGauge(new LongGauge.Config("category", "name")),
                new DefaultLongGauge(new LongGauge.Config("category", "name")),
                new DefaultLongGauge(new LongGauge.Config("category", "name")),
                new DefaultLongGauge(new LongGauge.Config("category", "name")));
    }

    /**
     * Creates a new {@link PublisherHandler.MetricsHolder} with default counters for testing.
     * These counters could be queried to verify the metrics' states.
     */
    private PublisherHandler.MetricsHolder generateHandlerMetrics() {
        return new PublisherHandler.MetricsHolder(
                new DefaultCounter(new Config("category", "name")),
                new DefaultCounter(new Config("category", "name")),
                new DefaultCounter(new Config("category", "name")),
                new DefaultCounter(new Config("category", "name")),
                new DefaultCounter(new Config("category", "name")),
                new DefaultCounter(new Config("category", "name")),
                new DefaultCounter(new Config("category", "name")));
    }
}
