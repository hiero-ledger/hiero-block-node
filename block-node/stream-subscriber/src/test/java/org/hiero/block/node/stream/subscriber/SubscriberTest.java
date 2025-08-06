// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hiero.block.api.BlockStreamSubscribeServiceInterface.BlockStreamSubscribeServiceMethod.subscribeBlockStream;
import static org.hiero.block.node.app.fixtures.TestUtils.enableDebugLogging;
import static org.hiero.block.node.app.fixtures.blocks.BlockItemUtils.toBlockItem;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.createNumberOfSimpleBlockBatches;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.ServiceInterface.Method;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.assertj.core.api.SoftAssertions;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.api.SubscribeStreamResponse;
import org.hiero.block.api.SubscribeStreamResponse.ResponseOneOfType;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.blockmessaging.BlockItems;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests for the SubscriberServicePlugin. It mocks out the rest of the block node so we can simply test just this
 * plugin.
 */
@SuppressWarnings("DataFlowIssue")
public class SubscriberTest extends GrpcPluginTestBase<SubscriberServicePlugin, ExecutorService> {
    private static final int RESPONSE_WAIT_LIMIT = 1000;

    private final SubscriberServicePlugin activePlugin;
    private final SimpleInMemoryHistoricalBlockFacility historicalFacility;

    /**
     * Constructor that creates new subscriber plugin and in-memory block facility.
     */
    public SubscriberTest() {
        super(Executors.newVirtualThreadPerTaskExecutor());
        historicalFacility = new SimpleInMemoryHistoricalBlockFacility();
        activePlugin = new SubscriberServicePlugin();
        start(activePlugin, subscribeBlockStream, historicalFacility, null);
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
    // @todo(1374): This test is disabled because the subscriber logic needs further validation.
    @Disabled("Disabled until GH Issue #1374 is fully implemented, subscriber logic must be validated!")
    @Test
    void testSubscribeAnyToMaxBlocksUnknown() throws ParseException {
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .startBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .endBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        // Need to wait for the handler to be registered...
        awaitSession();
        // should not have got a response
        assertEquals(0, fromPluginBytes.size());
        // check we can send some block items and they are received
        sendBatchesAndCheckTheyAreReceived(createNumberOfSimpleBlockBatches(1), 0);
        activePlugin.stop(); // request the plugin to end all client streams.
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
    // @todo(1374): This test is disabled because the subscriber logic needs further validation.
    @Disabled("Disabled until GH Issue #1374 is fully implemented, subscriber logic must be validated!")
    @Test
    void testSubscribeAnyToMaxBlocksZero() throws ParseException {
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .startBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .endBlockNumber(0)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        // Need to wait for the handler to be registered...
        awaitSession();
        // should not have got a response
        assertEquals(0, fromPluginBytes.size());
        // check we can send some block items and they are received
        sendBatchesAndCheckTheyAreReceived(createNumberOfSimpleBlockBatches(1), 0);
        awaitResponse(fromPluginBytes, 1);
        activePlugin.stop(); // request the plugin to end all client streams.
        assertEquals(2, fromPluginBytes.size());
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
    // @todo(1374): This test is disabled because the subscriber logic needs further validation.
    @Disabled("Disabled until GH Issue #1374 is fully implemented, subscriber logic must be validated!")
    @Test
    void testSubscribeAnyToMaxBlocksMax() throws ParseException {
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .startBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .endBlockNumber(Long.MAX_VALUE)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        // Need to wait for the handler to be registered...
        awaitSession();
        // should not have got a response yet
        assertEquals(0, fromPluginBytes.size());
        // check we can send some block items and they are received
        sendBatchesAndCheckTheyAreReceived(createNumberOfSimpleBlockBatches(10), 0);
        activePlugin.stop(); // request the plugin to end all client streams.
    }

    private void awaitSession() {
        int retries = 0;
        while (activePlugin.getOpenSessions().isEmpty() && retries < RESPONSE_WAIT_LIMIT) {
            // wait for the session to be established
            parkNanos(100_000L);
            retries++;
        }
        if (retries >= RESPONSE_WAIT_LIMIT) {
            System.out.println("Timed out waiting for session to be established");
        } else {
            System.out.printf(
                    "Session %d established.%n", activePlugin.getOpenSessions().size());
        }
    }

    @Test
    void testSubscriberBlockZero() throws ParseException {
        // Disable history so we really test live flow.
        historicalFacility.setDisablePlugin();
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .startBlockNumber(0)
                .endBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        // Need to wait for the handler to be registered...
        awaitSession();
        // check we can send some block items and they are received
        sendBatchesAndCheckTheyAreReceived(createNumberOfSimpleBlockBatches(25), 0);
        activePlugin.stop(); // request the plugin to end all client streams.
    }

    @Test
    void testSubscriberBlockZeroTwoChunks() throws ParseException {
        // Disable history so we really test live flow.
        historicalFacility.setDisablePlugin();
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .startBlockNumber(0)
                .endBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        // Need to wait for the handler to be registered...
        awaitSession();
        // check we can send some block items and they are received
        sendBatchesAndCheckTheyAreReceived(createNumberOfSimpleBlockBatches(0, 10), 0);
        sendBatchesAndCheckTheyAreReceived(createNumberOfSimpleBlockBatches(10, 20), 10);
        activePlugin.stop(); // request the plugin to end all client streams.
    }

    @Test
    void testSubscriberBlockStreamInMiddleNoHistory() throws ParseException {
        // Disable history so we really test live flow.
        historicalFacility.setDisablePlugin();
        // send first 10 items
        sendBatches(createNumberOfSimpleBlockBatches(0, 10), false, 0);
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .startBlockNumber(10)
                .endBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        // Need to wait for the handler to be registered...
        awaitSession();
        // check we did not get a bad response
        assertEquals(0, fromPluginBytes.size(), this::ErrorForSubscribeResponse);
        // check we can send some block items and they are received
        sendBatchesAndCheckTheyAreReceived(createNumberOfSimpleBlockBatches(10, 20), 0);
        activePlugin.stop(); // request the plugin to end all client streams.
    }

    @Test
    void testSubscriberBlockStreamInMiddleWithHistory() throws ParseException {
        // send first 10 items
        sendBatches(createNumberOfSimpleBlockBatches(0, 10), false, 0);
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .startBlockNumber(5)
                .endBlockNumber(19)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        // Need to wait for the handler to be registered...
        awaitSession();
        sendBatches(createNumberOfSimpleBlockBatches(10, 20), true, 6);
        activePlugin.stop(); // request the plugin to end all client streams.
        compareBatchesToResponses(createNumberOfSimpleBlockBatches(5, 20), 0, fromPluginBytes);
    }

    @Test
    void testSubscriberBlockStreamAheadOfMiddle() throws ParseException {
        // Disable history so we really test live flow.
        historicalFacility.setDisablePlugin();
        // send first 10 items
        sendBatches(createNumberOfSimpleBlockBatches(0, 10), false, 0);
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .startBlockNumber(15)
                .endBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        // Need to wait for the handler to be registered...
        awaitSession();
        // check we did not get a bad response
        assertEquals(0, fromPluginBytes.size(), this::ErrorForSubscribeResponse);
        // now send some blocks up to (but not including) the starting block
        sendBatches(createNumberOfSimpleBlockBatches(10, 15), false, 0);
        // check we can send some block items and they are received
        sendBatchesAndCheckTheyAreReceived(createNumberOfSimpleBlockBatches(15, 25), 0);
        activePlugin.stop(); // request the plugin to end all client streams.
    }

    @Disabled("Disabled until I rewrite this for the new approach.")
    @Test
    void testSubscribeFromZeroSlowClient() throws ParseException {
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .startBlockNumber(0)
                .endBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        // Need to wait for the handler to be registered...
        awaitSession();
        // check we can send some block items and they are received
        sendBatchesWithBackpressure(createNumberOfSimpleBlockBatches(0, 25), new int[] {5, 6, 7, 8, 9, 10}, 0);
    }

    // ==== Test bad response codes ====================================================================================

    @Test
    void testBadResponse() throws ParseException {
        // send first 10 items
        sendBatches(createNumberOfSimpleBlockBatches(0, 10), false, 0);
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .startBlockNumber(10000) // We allow a significant amount of future blocks, so make this larger.
                .endBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        // Just wait for the "bad request" response.
        awaitResponse(fromPluginBytes);
        activePlugin.stop(); // request the plugin to end all client streams.
        // check we did not get a bad response
        SubscribeStreamResponse response = parseResponse(fromPluginBytes.getFirst());
        assertEquals(ResponseOneOfType.STATUS, response.response().kind());
        assertEquals(SubscribeStreamResponse.Code.INVALID_START_BLOCK_NUMBER, response.status());
    }

    @Test
    void testBadResponseLargeNegativeStart() throws ParseException {
        // send first 10 items
        sendBatches(createNumberOfSimpleBlockBatches(0, 10), false, 0);
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .startBlockNumber(UNKNOWN_BLOCK_NUMBER)
                .endBlockNumber(UNKNOWN_BLOCK_NUMBER - 1)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        // Just wait for the "bad request" response.
        awaitResponse(fromPluginBytes);
        toPluginPipe.clientEndStreamReceived();
        // check we got a response, but did not get an incorrect response
        assertThat(fromPluginBytes.size())
                .as("Expected at least one response, but got 0.")
                .isGreaterThan(0);
        SubscribeStreamResponse response = parseResponse(fromPluginBytes.getFirst());
        assertEquals(ResponseOneOfType.STATUS, response.response().kind());
        assertEquals(SubscribeStreamResponse.Code.INVALID_END_BLOCK_NUMBER, response.status());
    }

    @Test
    void testBadResponseEndBeforeStart() {
        // send first 10 items
        sendBatches(createNumberOfSimpleBlockBatches(0, 10), false, 0);
        // first we need to create and send a SubscribeStreamRequest
        final SubscribeStreamRequest subscribeStreamRequest = SubscribeStreamRequest.newBuilder()
                .startBlockNumber(10)
                .endBlockNumber(5)
                .build();
        toPluginPipe.onNext(SubscribeStreamRequest.PROTOBUF.toBytes(subscribeStreamRequest));
        // Just wait for the "bad request" response.
        awaitResponse(fromPluginBytes);
        toPluginPipe.clientEndStreamReceived();
        // check we did get a bad response
        assertThat(fromPluginBytes.size()).isGreaterThan(0);
        SubscribeStreamResponse response = parseResponse(fromPluginBytes.getFirst());
        assertEquals(ResponseOneOfType.STATUS, response.response().kind());
        assertEquals(SubscribeStreamResponse.Code.INVALID_END_BLOCK_NUMBER, response.status());
    }

    // ==== Testing Utility Methods ====================================================================================

    private String ErrorForSubscribeResponse() {
        try {
            final SubscribeStreamResponse actualResponse = parseResponse(fromPluginBytes.getFirst());
            return "Expected no response but got %s.".formatted(actualResponse);
        } catch (final RuntimeException e) {
            return "Expected no response and unable to parse actual response. %s.".formatted(e);
        }
    }

    private void awaitResponse(List<Bytes> fromPluginBytes) {
        awaitResponse(fromPluginBytes, 1);
    }

    private void awaitResponse(List<Bytes> fromPluginBytes, int requiredReplies) {
        int retries = 0;
        parkNanos(100_000L); // Always wait at least once.
        while (fromPluginBytes.size() < requiredReplies && retries < RESPONSE_WAIT_LIMIT) {
            // wait for a response
            parkNanos(100_000L);
            retries++;
        }
        if (retries >= RESPONSE_WAIT_LIMIT) {
            System.out.printf("Timed out waiting for %d responses%n", requiredReplies);
        }
    }

    /**
     * Test the subscriber service, sending blocks to the messaging facility.
     */
    void sendBatchesWithoutChecks(final BlockItems[] batches, final int expectedResponses) {
        final int offset = fromPluginBytes.size();
        // send all the block items
        sendBatches(batches, true, 0);
        awaitResponse(fromPluginBytes, expectedResponses);
    }

    /**
     * Test the subscriber service, sending 25 blocks to the messaging facility and checking they are received
     * correctly.
     */
    void sendBatchesAndCheckTheyAreReceived(final BlockItems[] batches, int responsesAlreadyReceived) {
        final int offset = responsesAlreadyReceived;
        System.out.printf("Sending %d batches after receiving %d%n", batches.length, offset);
        // send all the block items
        sendBatches(batches, true, offset);
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
        // sendBatches(batches, blocksToSlow, null, sessionToSlow);
        // check we got all the items
        final int responseCount = fromPluginBytes.size() - offset;
        assertThat(responseCount)
                .as("Expected %d responses, but got %d.".formatted(batches.length, responseCount))
                .isEqualTo(batches.length);
        compareBatchesToResponses(batches, offset, fromPluginBytes);
    }

    private void compareBatchesToResponses(
            final BlockItems[] batches, final int offset, final List<Bytes> responseBytes) {
        final int responseCount = responseBytes.size() - offset;
        SoftAssertions.assertSoftly(softly -> {
            // Note that we allow extra responses mostly so that we accommodate a close/complete message.
            softly.assertThat(responseCount)
                    .as("Expected at least %d responses, but got %d.".formatted(batches.length, responseCount))
                    .isGreaterThanOrEqualTo(batches.length);
            for (int i = 0; i < batches.length && i < responseCount; i++) {
                SubscribeStreamResponse response = parseResponse(responseBytes.get(i + offset));
                softly.assertThat(response.response().kind())
                        .as("Expected BLOCK_ITEMS but got " + response.response())
                        .isEqualTo(ResponseOneOfType.BLOCK_ITEMS);
                BlockItems next = batches[i];
                final List<BlockItem> returned = response.blockItems().blockItems();
                final List<BlockItemUnparsed> sent = next.blockItems();
                for (int j = 0; j < sent.size(); j++) {
                    final BlockItem expectedItem = toBlockItem(sent.get(j));
                    final BlockItem actualItem = returned.get(j);
                    softly.assertThat(actualItem)
                            .as("Failed to match batch %d, block %d, Item %d.".formatted(i, next.newBlockNumber(), j))
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

    void sendBatches(final BlockItems[] batches, final boolean waitForResponses, int previousResponses) {
        for (int i = 0; i < batches.length; i++) {
            final BlockItems batch = batches[i];
            blockMessaging.sendBlockItems(batch);
            // minimal delay (10 μs) so we don't stream too fast.
            parkNanos(10_000L);
        }
        if (waitForResponses && batches.length >= 1) {
            awaitResponse(fromPluginBytes, batches.length + previousResponses);
        }
    }
}
