// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.subscriber;

import static org.hiero.block.node.app.fixtures.TestUtils.enableDebugLogging;
import static org.hiero.block.node.app.fixtures.blocks.BlockItemUtils.toBlockItemsUnparsed;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;
import static org.hiero.block.node.subscriber.SubscriberServicePlugin.BlockStreamSubscriberServiceMethod.subscribeBlockStream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.ServiceInterface.Method;
import java.util.Arrays;
import java.util.List;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.hiero.block.node.spi.blockmessaging.NoBackPressureBlockItemHandler;
import org.hiero.hapi.block.node.SubscribeStreamRequest;
import org.hiero.hapi.block.node.SubscribeStreamResponse;
import org.hiero.hapi.block.node.SubscribeStreamResponse.ResponseOneOfType;
import org.hiero.hapi.block.node.SubscribeStreamResponseCode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the SubscriberServicePlugin. It mocks out the rest of the block node so we can simply test just this
 * plugin.
 */
@SuppressWarnings("DataFlowIssue")
public class SubscriberTest extends GrpcPluginTestBase<SubscriberServicePlugin> {

    private final SubscriberServicePlugin activePlugin;

    /**
     * Constructor that creates new subscriber plugin and in-memory block facility.
     */
    public SubscriberTest() {
        super(new SubscriberServicePlugin(), subscribeBlockStream, new SimpleInMemoryHistoricalBlockFacility());
        activePlugin = plugin; // the base class variable name is a bit too generic...
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
     * and makes sure they are delivered correctly.
     *
     * @throws ParseException should not happen
     */
    @Test
    void testSubscriberUnknownBlock() throws ParseException {
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .allowUnverified(true)
                .startBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .endBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        // check we can send some block items and they are received
        sendBlocksAndCheckTheyAreReceived(createNumberOfVerySimpleBlocks(25));
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
        // check we can send some block items and they are received
        sendBlocksAndCheckTheyAreReceived(createNumberOfVerySimpleBlocks(25));
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
        // check we can send some block items and they are received
        sendBlocksAndCheckTheyAreReceived(createNumberOfVerySimpleBlocks(0, 10));
        sendBlocksAndCheckTheyAreReceived(createNumberOfVerySimpleBlocks(10, 20));
    }

    @Test
    void testSubscriberBlockStreamInMiddle() throws ParseException {
        // send first 10 items
        sendBlocks(createNumberOfVerySimpleBlocks(0, 10));
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .allowUnverified(true)
                .startBlockNumber(10)
                .endBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        // check we did not get a bad response
        assertEquals(0, fromPluginBytes.size(), this::ErrorForSubscribeResponse);
        // check we can send some block items and they are received
        sendBlocksAndCheckTheyAreReceived(createNumberOfVerySimpleBlocks(10, 20));
    }

    private String ErrorForSubscribeResponse() {
        try {
            final SubscribeStreamResponse actualResponse =
                    SubscribeStreamResponse.PROTOBUF.parse(fromPluginBytes.getFirst());
            return "Expected no response but got %s.".formatted(actualResponse);
        } catch (final ParseException e) {
            return "Expected no response and unable to parse actual response. %s.".formatted(e);
        }
    }

    @Test
    void testSubscriberBlockStreamAheadOfMiddle() throws ParseException {
        // send first 10 items
        sendBlocks(createNumberOfVerySimpleBlocks(0, 10));
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .allowUnverified(true)
                .startBlockNumber(15)
                .endBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        // check we did not get a bad response
        assertEquals(0, fromPluginBytes.size(), this::ErrorForSubscribeResponse);
        // now send some blocks up the starting block
        sendBlocks(createNumberOfVerySimpleBlocks(10, 15));
        // check we can send some block items and they are received
        sendBlocksAndCheckTheyAreReceived(createNumberOfVerySimpleBlocks(15, 25));
    }

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
        sendBlocksWithBackpressure(createNumberOfVerySimpleBlocks(0, 15), new int[] {5, 6, 7, 8, 9, 10}, 0);
        sendBlocksWithBackpressure(createNumberOfVerySimpleBlocks(15, 25), null, -1);
    }

    // ==== Test bad response codes ====================================================================================

    @Test
    void testBadResponse() throws ParseException {
        // send first 10 items
        sendBlocks(createNumberOfVerySimpleBlocks(0, 10));
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .allowUnverified(true)
                .startBlockNumber(1000)
                .endBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        // check we did not get a bad response
        SubscribeStreamResponse response = SubscribeStreamResponse.PROTOBUF.parse(fromPluginBytes.getFirst());
        assertEquals(ResponseOneOfType.STATUS, response.response().kind());
        assertEquals(SubscribeStreamResponseCode.READ_STREAM_INVALID_START_BLOCK_NUMBER, response.status());
    }

    @Test
    void testBadResponseLargeNegativeStart() throws ParseException {
        // send first 10 items
        sendBlocks(createNumberOfVerySimpleBlocks(0, 10));
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .allowUnverified(true)
                .startBlockNumber(-1)
                .endBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        // check we did not get a bad response
        SubscribeStreamResponse response = SubscribeStreamResponse.PROTOBUF.parse(fromPluginBytes.getFirst());
        assertEquals(ResponseOneOfType.STATUS, response.response().kind());
        assertEquals(SubscribeStreamResponseCode.READ_STREAM_INVALID_START_BLOCK_NUMBER, response.status());
    }

    @Test
    void testBadResponseEndBeforeStart() throws ParseException {
        // send first 10 items
        sendBlocks(createNumberOfVerySimpleBlocks(0, 10));
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .allowUnverified(true)
                .startBlockNumber(10)
                .endBlockNumber(5)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        // check we did not get a bad response
        SubscribeStreamResponse response = SubscribeStreamResponse.PROTOBUF.parse(fromPluginBytes.getFirst());
        assertEquals(ResponseOneOfType.STATUS, response.response().kind());
        assertEquals(SubscribeStreamResponseCode.READ_STREAM_INVALID_END_BLOCK_NUMBER, response.status());
    }

    // ==== Testing Utility Methods ====================================================================================

    /**
     * Test the subscriber service, sending 25 blocks to the messaging facility and checking they are received
     * correctly.
     */
    void sendBlocksAndCheckTheyAreReceived(final BlockItem[] blockItems) throws ParseException {
        final int offset = fromPluginBytes.size();
        // send all the block items
        sendBlocks(blockItems);
        // check we got all the items
        assertEquals(blockItems.length, fromPluginBytes.size() - offset);
        for (int i = 0; i < blockItems.length; i++) {
            SubscribeStreamResponse response = SubscribeStreamResponse.PROTOBUF.parse(fromPluginBytes.get(i + offset));
            assertEquals(
                    ResponseOneOfType.BLOCK_ITEMS,
                    response.response().kind(),
                    "Expected BLOCK_ITEMS but got " + response.response());
            assertEquals(blockItems[i], response.blockItems().blockItems().getFirst());
        }
    }

    /**
     * Test the subscriber service, sending blocks to the messaging facility.
     * At indicated blocks, apply backpressure (as though the client is slow)
     */
    void sendBlocksWithBackpressure(final BlockItem[] blockItems, final int[] blocksToSlow, final int sessionToSlow)
            throws ParseException {
        final int offset = fromPluginBytes.size();
        // send all the block items
        sendBlocks(blockItems, blocksToSlow, null, sessionToSlow);
        // check we got all the items
        assertEquals(blockItems.length, fromPluginBytes.size() - offset);
        for (int i = 0; i < blockItems.length; i++) {
            SubscribeStreamResponse response = SubscribeStreamResponse.PROTOBUF.parse(fromPluginBytes.get(i + offset));
            assertEquals(
                    ResponseOneOfType.BLOCK_ITEMS,
                    response.response().kind(),
                    "Expected BLOCK_ITEMS but got " + response.response());
            assertEquals(blockItems[i], response.blockItems().blockItems().getFirst());
        }
    }

    /**
     * Send the given block items to messaging service.
     */
    void sendBlocks(final BlockItem[] blockItems) {
        final int[] empty = {};
        sendBlocks(blockItems, empty, null, -1);
    }

    /**
     * Send the given block items to the messaging service.
     *
     * @param blockItems An array of block items to send one at a time
     * @param blocksToApplyBackpressure an array of index values, at each index value we will apply backpressure
     *         to the messaging service for the subscriber handler and possibly test for an exception.
     *         An empty array means to not apply backpressure.
     * @param expectedException An exception expected when backpressure is applied, null if no exception
     *         is expected.
     */
    void sendBlocks(
            final BlockItem[] blockItems,
            final int[] blocksToApplyBackpressure,
            final Class<? extends Throwable> expectedException,
            final int sessionIndexToSlow) {
        // send all the block items
        Arrays.sort(blocksToApplyBackpressure);
        final NoBackPressureBlockItemHandler[] allSessions = activePlugin.getOpenSessions();
        final NoBackPressureBlockItemHandler handlerToSlow;
        if (sessionIndexToSlow >= 0 && allSessions.length > sessionIndexToSlow) {
            handlerToSlow = allSessions[sessionIndexToSlow];
        } else {
            handlerToSlow = null;
        }
        long blockNumber = UNKNOWN_BLOCK_NUMBER;
        boolean blockedPriorItem = false;
        for (int i = 0; i < blockItems.length; i++) {
            final BlockItem blockItem = blockItems[i];
            final boolean isBlockedItem =
                    handlerToSlow != null && Arrays.binarySearch(blocksToApplyBackpressure, i) >= 0;
            if (isBlockedItem) {
                blockMessaging.setHandlerBehind(handlerToSlow);
                blockedPriorItem = true;
            } else if (blockedPriorItem) {
                blockMessaging.clearBackpressure(handlerToSlow);
                blockedPriorItem = false;
            }
            blockNumber = blockItem.hasBlockHeader() ? blockItem.blockHeader().number() : blockNumber;
            final BlockItems itemsToSend = new BlockItems(toBlockItemsUnparsed(blockItem), blockNumber);
            if (isBlockedItem && expectedException != null) {
                Assertions.assertThrows(expectedException, () -> blockMessaging.sendBlockItems(itemsToSend));
            } else {
                blockMessaging.sendBlockItems(itemsToSend);
            }
        }
    }
}
