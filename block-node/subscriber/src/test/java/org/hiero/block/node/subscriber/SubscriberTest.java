// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.subscriber;

import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hiero.block.node.app.fixtures.TestUtils.enableDebugLogging;
import static org.hiero.block.node.app.fixtures.blocks.BlockItemUtils.toBlockItem;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.createNumberOfSimpleBlockBatches;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;
import static org.hiero.block.node.subscriber.SubscriberServicePlugin.SubscriberServiceMethod.subscribeBlockStream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.ServiceInterface.Method;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.Arrays;
import java.util.List;
import org.assertj.core.api.SoftAssertions;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.api.SubscribeStreamResponse.ResponseOneOfType;
import org.hiero.block.api.SubscribeStreamResponse.SubscribeStreamResponseCode;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.NoBackPressureBlockItemHandler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests for the SubscriberServicePlugin. It mocks out the rest of the block node so we can simply test just this
 * plugin.
 */
@SuppressWarnings("DataFlowIssue")
public class SubscriberTest extends GrpcPluginTestBase<SubscriberServicePlugin> {

    private final SubscriberServicePlugin activePlugin;
    private final SimpleInMemoryHistoricalBlockFacility historicalFacility;

    /**
     * Constructor that creates new subscriber plugin and in-memory block facility.
     */
    public SubscriberTest() {
        super(new SubscriberServicePlugin(), subscribeBlockStream, new SimpleInMemoryHistoricalBlockFacility());
        activePlugin = plugin; // the base class variable name is a bit too generic...
        // We need the test value, not just the generic interface.
        // This would be MUCH cleaner with Java 23 (we could assign the attribute _then_ call super)
        historicalFacility = (SimpleInMemoryHistoricalBlockFacility) blockNodeContext.historicalBlockProvider();
    }

    /**
     * Enable debug logging for each test.
     */
    @BeforeEach
    void setup() {
        // enable debug System.logger logging
        enableDebugLogging();
    }

    /**
     * Test the service methods are correctly defined.
     */
    @Test
    void testServiceInterfaceBasics() {
        // check we have a service interface
        assertNotNull(serviceInterface);
        // check the methods from service interface
        List<Method> methods = serviceInterface.methods();
        assertNotNull(methods);
        assertEquals(1, methods.size());
        assertEquals(subscribeBlockStream, methods.getFirst());
    }

    /**
     * Test the subscriber service, create a single subscriber and send it some block items via the messaging services
     * and makes sure they are delivered correctly. Tests block range:
     * <ul>
     *     <li>Starting at block UNKNOWN_BLOCK_NUMBER which means any block</li>
     *     <li>Ending at block UNKNOWN_BLOCK_NUMBER which means stream forever</li>
     * </ul>
     *
     * @throws ParseException should not happen
     */
    @Test
    void testSubscribeAnyToMaxBlocksUnknown() throws ParseException {
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .allowUnverified(true)
                .startBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .endBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        toPluginPipe.clientEndStreamReceived();
        // should not have got a response
        assertEquals(0, fromPluginBytes.size());
        // check we can send some block items and they are received
        sendBatchesAndCheckTheyAreReceived(createNumberOfSimpleBlockBatches(1));
    }

    /**
     * Test the subscriber service, create a single subscriber and send it some block items via the messaging services
     * and makes sure they are delivered correctly. Tests block range:
     * <ul>
     *     <li>Starting at block UNKNOWN_BLOCK_NUMBER which means any block</li>
     *     <li>Ending at block 0 which means stream forever</li>
     * </ul>
     *
     * @throws ParseException should not happen
     */
    @Test
    void testSubscribeAnyToMaxBlocksZero() throws ParseException {
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .allowUnverified(true)
                .startBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .endBlockNumber(0)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        toPluginPipe.clientEndStreamReceived();
        // should not have got a response
        assertEquals(0, fromPluginBytes.size());
        // check we can send some block items and they are received
        sendBatchesAndCheckTheyAreReceived(createNumberOfSimpleBlockBatches(1));
    }

    /**
     * Test the subscriber service, create a single subscriber and send it some block items via the messaging services
     * and makes sure they are delivered correctly. Tests block range:
     * <ul>
     *     <li>Starting at block UNKNOWN_BLOCK_NUMBER which means any block</li>
     *     <li>Ending at block Long.MAX_VALUE which means stream forever</li>
     * </ul>
     *
     * @throws ParseException should not happen
     */
    @Test
    void testSubscribeAnyToMaxBlocksMax() throws ParseException {
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .allowUnverified(true)
                .startBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .endBlockNumber(Long.MAX_VALUE)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        toPluginPipe.clientEndStreamReceived();
        // should not have got a response
        assertEquals(0, fromPluginBytes.size());
        // check we can send some block items and they are received
        sendBatchesAndCheckTheyAreReceived(createNumberOfSimpleBlockBatches(1));
    }

    @Test
    void testSubscriberBlockZero() throws ParseException {
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .allowUnverified(true)
                .startBlockNumber(0)
                .endBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        toPluginPipe.clientEndStreamReceived();
        // check we can send some block items and they are received
        sendBatchesAndCheckTheyAreReceived(createNumberOfSimpleBlockBatches(25));
    }

    @Test
    void testSubscriberBlockZeroTwoChunks() throws ParseException {
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .allowUnverified(true)
                .startBlockNumber(0)
                .endBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        toPluginPipe.clientEndStreamReceived();
        // check we can send some block items and they are received
        sendBatchesAndCheckTheyAreReceived(createNumberOfSimpleBlockBatches(0, 10));
        sendBatchesAndCheckTheyAreReceived(createNumberOfSimpleBlockBatches(10, 20));
    }

    @Test
    void testSubscriberBlockStreamInMiddle() throws ParseException {
        // send first 10 items
        sendBatches(createNumberOfSimpleBlockBatches(0, 10));
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .allowUnverified(true)
                .startBlockNumber(10)
                .endBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        toPluginPipe.clientEndStreamReceived();
        // check we did not get a bad response
        assertEquals(0, fromPluginBytes.size(), this::ErrorForSubscribeResponse);
        // check we can send some block items and they are received
        sendBatchesAndCheckTheyAreReceived(createNumberOfSimpleBlockBatches(10, 20));
    }

    private String ErrorForSubscribeResponse() {
        try {
            final SubscribeStreamResponse actualResponse = parseResponse(fromPluginBytes.getFirst());
            return "Expected no response but got %s.".formatted(actualResponse);
        } catch (final RuntimeException e) {
            return "Expected no response and unable to parse actual response. %s.".formatted(e);
        }
    }

    @Test
    void testSubscriberBlockStreamAheadOfMiddle() throws ParseException {
        // send first 10 items
        sendBatches(createNumberOfSimpleBlockBatches(0, 10));
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .allowUnverified(true)
                .startBlockNumber(15)
                .endBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        toPluginPipe.clientEndStreamReceived();
        // check we did not get a bad response
        assertEquals(0, fromPluginBytes.size(), this::ErrorForSubscribeResponse);
        // now send some blocks up the starting block
        sendBatches(createNumberOfSimpleBlockBatches(10, 15));
        // check we can send some block items and they are received
        sendBatchesAndCheckTheyAreReceived(createNumberOfSimpleBlockBatches(15, 25));
    }

    @Disabled
    @Test
    void testSubscribeFromZeroSlowClient() throws ParseException {
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .allowUnverified(true)
                .startBlockNumber(0)
                .endBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        // check we can send some block items and they are received
        sendBatchesWithBackpressure(createNumberOfSimpleBlockBatches(0, 25), new int[] {5, 6, 7, 8, 9, 10}, 0);
    }

    // ==== Test bad response codes ====================================================================================

    @Test
    void testBadResponse() throws ParseException {
        // send first 10 items
        sendBatches(createNumberOfSimpleBlockBatches(0, 10));
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .allowUnverified(true)
                .startBlockNumber(1000)
                .endBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        // check we did not get a bad response
        SubscribeStreamResponse response = parseResponse(fromPluginBytes.getFirst());
        assertEquals(ResponseOneOfType.STATUS, response.response().kind());
        assertEquals(SubscribeStreamResponseCode.READ_STREAM_INVALID_START_BLOCK_NUMBER, response.status());
    }

    @Test
    void testBadResponseLargeNegativeStart() throws ParseException {
        // send first 10 items
        sendBatches(createNumberOfSimpleBlockBatches(0, 10));
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .allowUnverified(true)
                .startBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .endBlockNumber(UNKNOWN_BLOCK_NUMBER - 1)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        // check we got a response, but did not get an incorrect response
        assertThat(fromPluginBytes.size())
                .as("Expected at least one response, but got 0.")
                .isGreaterThan(0);
        SubscribeStreamResponse response = parseResponse(fromPluginBytes.getFirst());
        assertEquals(ResponseOneOfType.STATUS, response.response().kind());
        assertEquals(SubscribeStreamResponseCode.READ_STREAM_INVALID_END_BLOCK_NUMBER, response.status());
    }

    @Test
    void testBadResponseEndBeforeStart() {
        // send first 10 items
        sendBatches(createNumberOfSimpleBlockBatches(0, 10));
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .allowUnverified(true)
                .startBlockNumber(10)
                .endBlockNumber(5)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        toPluginPipe.clientEndStreamReceived();
        // check we did not get a bad response
        assertThat(fromPluginBytes.size()).isGreaterThan(0);
        SubscribeStreamResponse response = parseResponse(fromPluginBytes.getFirst());
        assertEquals(ResponseOneOfType.STATUS, response.response().kind());
        assertEquals(SubscribeStreamResponseCode.READ_STREAM_INVALID_END_BLOCK_NUMBER, response.status());
    }

    // ==== Testing Utility Methods ====================================================================================

    /**
     * Test the subscriber service, sending 25 blocks to the messaging facility and checking they are received
     * correctly.
     */
    void sendBatchesAndCheckTheyAreReceived(final BlockItems[] batches) {
        final int offset = fromPluginBytes.size();
        // send all the block items
        sendBatches(batches);
        // check we got all the items
        final int responseCount = fromPluginBytes.size() - offset;
        // assertEquals(batches.length, responseCount);
        compareBatchesToResponses(batches, offset, fromPluginBytes);
    }

    /**
     * Test the subscriber service, sending blocks to the messaging facility.
     * At indicated blocks, apply backpressure (as though the client is slow)
     */
    void sendBatchesWithBackpressure(final BlockItems[] batches, final int[] blocksToSlow, final int sessionToSlow)
            throws ParseException {
        final int offset = fromPluginBytes.size();
        // send all the block items
        sendBatches(batches, blocksToSlow, null, sessionToSlow);
        // check we got all the items
        final int responseCount = fromPluginBytes.size() - offset;
        assertThat(responseCount)
                .as("Expected %d responses, but got %d.".formatted(batches.length, responseCount))
                .isEqualTo(batches.length);
        compareBatchesToResponses(batches, offset, fromPluginBytes);
    }

    private void compareBatchesToResponses(
            final BlockItems[] batches, final int offset, final List<Bytes> responseBytes) {
        final int responseCount = responseBytes.size();
        SoftAssertions.assertSoftly(softly -> {
            for (int i = 0; i < batches.length && i < responseCount; i++) {
                SubscribeStreamResponse response = parseResponse(responseBytes.get(i + offset));
                softly.assertThat(response.response().kind())
                        .as("Expected BLOCK_ITEMS but got " + response.response())
                        .isEqualTo(ResponseOneOfType.BLOCK_ITEMS);
                BlockItems next = batches[i];
                final List<BlockItem> returned = response.blockItems().blockItems();
                final List<BlockItemUnparsed> sent = next.blockItems();
                for (int u = 0; u < sent.size(); u++) {
                    final BlockItem expectedItem = toBlockItem(sent.get(u));
                    final BlockItem actualItem = returned.get(u);
                    softly.assertThat(actualItem)
                            .as("Failed to match batch %d, block %d, Item %d.".formatted(i, next.newBlockNumber(), u))
                            .isEqualTo(expectedItem);
                }
            }
        });
    }

    private SubscribeStreamResponse parseResponse(Bytes responseToParse) {
        try {
            return SubscribeStreamResponse.PROTOBUF.parse(responseToParse);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    private int countBlocks(final BlockItems[] batches) {
        return Arrays.stream(batches).mapToInt((a) -> a.blockItems().size()).sum();
    }

    void sendBatches(final BlockItems[] batches) {
        sendBatches(batches, null, null, -1);
    }

    /**
     * Send the given block items to the messaging service.
     *
     * @param batches An array of block items to send one at a time
     * @param batchesToApplyBackpressure an array of index values, at each index value we will apply backpressure
     *         to the messaging service for the subscriber handler and possibly test for an exception.
     *         An empty array means to not apply backpressure.
     * @param expectedException An exception expected when backpressure is applied, null if no exception
     *         is expected.
     */
    void sendBatches(
            final BlockItems[] batches,
            final int[] batchesToApplyBackpressure,
            final Class<? extends Throwable> expectedException,
            final int sessionIndexToSlow) {
        // Sort the backpressure array, if present, so we can binary search it.
        if (batchesToApplyBackpressure != null) {
            Arrays.sort(batchesToApplyBackpressure);
        }
        // send all the block items
        final NoBackPressureBlockItemHandler[] allSessions = activePlugin.getOpenSessions();
        final NoBackPressureBlockItemHandler handlerToSlow;
        if (sessionIndexToSlow >= 0 && allSessions.length > sessionIndexToSlow) {
            handlerToSlow = allSessions[sessionIndexToSlow];
        } else {
            handlerToSlow = null;
        }
        long lastBlockNumberSent = UNKNOWN_BLOCK_NUMBER;
        boolean blockedPriorItem = false;
        for (int i = 0; i < batches.length; i++) {
            final BlockItems batch = batches[i];
            final boolean isBlockedItem =
                    handlerToSlow != null && Arrays.binarySearch(batchesToApplyBackpressure, i) >= 0;
            if (isBlockedItem && !blockedPriorItem) {
                historicalFacility.setDelayResponses();
                blockMessaging.setHandlerBehind(handlerToSlow);
                blockedPriorItem = true;
            } else if (!isBlockedItem && blockedPriorItem) {
                historicalFacility.clearDelayResponses();
                blockedPriorItem = false;
            }
            if (isBlockedItem && expectedException != null) {
                Assertions.assertThrows(expectedException, () -> blockMessaging.sendBlockItems(batch));
            } else {
                blockMessaging.sendBlockItems(batch);
            }
            lastBlockNumberSent = batch.newBlockNumber() >= 0 ? batch.newBlockNumber() : lastBlockNumberSent;
        }
        // Note: the following is slow and uses sleeps, so it would be good to find a better
        // option for synchronizing the session thread in the future.
        if (handlerToSlow instanceof BlockStreamSubscriberSession subscriberSession) {
            // Wait 250 μs at a time for the session to catch up, but limit to 2.5s total
            int n = 0;
            while (n < 10_000 && subscriberSession.isHistoryStreamActive()) {
                parkNanos(250_000);
                n++;
            }
            if (n >= 9_000) {
                // We waited too long, report that.
                System.out.println(
                        "%n%n%nSubscriber session %s took too long to catch up.%n%n%n".formatted(subscriberSession));
            }
            // check that live is done.
            // Wait 250 μs at a time for the session to finish, but limit to 2.5s total
            while (n < 10_000 && subscriberSession.getCurrentBlockBeingSent() < lastBlockNumberSent) {
                parkNanos(250_000);
                n++;
            }
            if (n >= 9_000) {
                // We waited too long, report that.
                System.out.println("%n%n%nSubscriber session %s took too long to finish.%n Current %d < last %d.%n%n%n"
                        .formatted(
                                subscriberSession, subscriberSession.getCurrentBlockBeingSent(), lastBlockNumberSent));
            }
            // Wait 500 μs more for the last bit to get through
            parkNanos(500_000);
        }
    }
}
