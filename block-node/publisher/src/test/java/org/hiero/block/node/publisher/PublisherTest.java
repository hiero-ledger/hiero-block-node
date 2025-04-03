// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.publisher;

import static com.hedera.hapi.block.PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT;
import static com.hedera.hapi.block.PublishStreamResponse.ResponseOneOfType.STATUS;
import static org.hiero.block.node.app.fixtures.TestUtils.enableDebugLogging;
import static org.hiero.block.node.app.fixtures.blocks.BlockItemUtils.toBlockItemJson;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.sampleBlockHeader;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.sampleBlockProof;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.sampleRoundHeader;
import static org.hiero.block.node.publisher.PublisherServicePlugin.BlockStreamPublisherServiceMethod.publishBlockStream;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.hedera.hapi.block.Acknowledgement;
import com.hedera.hapi.block.BlockAcknowledgement;
import com.hedera.hapi.block.BlockItemSet;
import com.hedera.hapi.block.PublishStreamRequest;
import com.hedera.hapi.block.PublishStreamRequest.RequestOneOfType;
import com.hedera.hapi.block.PublishStreamResponse;
import com.hedera.hapi.block.PublishStreamResponseCode;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.ServiceInterface.Method;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.util.List;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.NoBlocksHistoricalBlockFacility;
import org.hiero.block.node.spi.blockmessaging.BlockNotification;
import org.hiero.block.node.spi.blockmessaging.BlockNotification.Type;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for the PublisherServicePlugin. It mocks out the rest of the block node so we can simply test just this plugin.
 */
@SuppressWarnings({"FieldCanBeLocal", "MismatchedQueryAndUpdateOfCollection", "SameParameterValue"})
public class PublisherTest extends GrpcPluginTestBase<PublisherServicePlugin> {

    public PublisherTest() {
        super(new PublisherServicePlugin(), publishBlockStream, new NoBlocksHistoricalBlockFacility());
    }

    @Test
    void testServiceInterfaceBasics() {
        // check we have a service interface
        assertNotNull(serviceInterface);
        // check the methods from service interface
        List<Method> methods = serviceInterface.methods();
        assertNotNull(methods);
        assertEquals(1, methods.size());
        assertEquals(publishBlockStream, methods.getFirst());
    }

    @Test
    void testPublisher() {
        // enable debug System.logger logging
        enableDebugLogging();
        // create some sample data to send to plugin
        final BlockItem blockHeader1 = sampleBlockHeader(0);
        final Bytes publishBlockHeader1StreamRequest = blockItemsToPublishStreamRequest(blockHeader1);
        final BlockItem roundHeader1 = sampleRoundHeader(2);
        final Bytes publishRoundHeader1StreamRequest = blockItemsToPublishStreamRequest(roundHeader1);
        final BlockItem blockProof1 = sampleBlockProof(0);
        final Bytes publishBlockProof1StreamRequest = blockItemsToPublishStreamRequest(blockProof1);
        // send the data to the plugin
        toPluginPipe.onNext(publishBlockHeader1StreamRequest);
        toPluginPipe.onNext(publishRoundHeader1StreamRequest);
        toPluginPipe.onNext(publishBlockProof1StreamRequest);
        // check the data was sent through to the block messaging facility
        assertEquals(3, blockMessaging.getSentBlockItems().size());
        assertEquals(0, blockMessaging.getSentBlockNotifications().size());
        assertEquals(
                toBlockItemJson(blockHeader1),
                toBlockItemJson(
                        blockMessaging.getSentBlockItems().get(0).blockItems().getFirst()));
        assertEquals(0, blockMessaging.getSentBlockItems().get(0).newBlockNumber());
        assertEquals(
                toBlockItemJson(roundHeader1),
                toBlockItemJson(
                        blockMessaging.getSentBlockItems().get(1).blockItems().getFirst()));
        assertEquals(
                UNKNOWN_BLOCK_NUMBER, blockMessaging.getSentBlockItems().get(1).newBlockNumber());
        assertEquals(
                toBlockItemJson(blockProof1),
                toBlockItemJson(
                        blockMessaging.getSentBlockItems().get(2).blockItems().getFirst()));
        assertEquals(
                UNKNOWN_BLOCK_NUMBER, blockMessaging.getSentBlockItems().get(2).newBlockNumber());
    }

    /**
     * Test that when the publisher receives a block notification of type BLOCK_PERSISTED, it sends a
     * BlockAcknowledgement to all connected consensus nodes.
     *
     * @throws ParseException should not happen
     */
    @Test
    void testPublisherSendsOnBlockPersistedNotification() throws ParseException {
        blockMessaging.sendBlockNotification(new BlockNotification(100, Type.BLOCK_PERSISTED, null));
        // check the data was sent through to the test block messaging facility
        assertEquals(1, blockMessaging.getSentBlockNotifications().size());
        // check if a response was sent to CN
        assertEquals(1, fromPluginBytes.size());
        PublishStreamResponse response = PublishStreamResponse.PROTOBUF.parse(fromPluginBytes.getFirst());
        assertEquals(ACKNOWLEDGEMENT, response.response().kind());
        final Acknowledgement ack = response.response().as();
        assertEquals(new BlockAcknowledgement(100, null, false), ack.blockAck());
    }

    @Test
    @DisplayName(
            "When publisher receives a Duplicated (already acked) block, it should send an ack with duplicate flag")
    void testPublisherDuplicateBlock() throws IOException, ParseException {
        // create sample block 1
        final BlockItem blockHeader1 = sampleBlockHeader(1);
        toPluginPipe.onNext(blockItemsToPublishStreamRequest(blockHeader1));
        // check the data was sent through to the block messaging facility
        assertEquals(1, blockMessaging.getSentBlockItems().size());
        // simulate verification success
        blockMessaging.sendBlockNotification(new BlockNotification(1, Type.BLOCK_VERIFIED, Bytes.wrap("hash1")));
        // simulate persistence success
        blockMessaging.sendBlockNotification(new BlockNotification(1, Type.BLOCK_PERSISTED, null));
        // re-attempt to send same block.

        final BlockItem blockHeader1Duplicate = sampleBlockHeader(0);
        toPluginPipe.onNext(blockItemsToPublishStreamRequest(blockHeader1Duplicate));

        assertEquals(2, fromPluginBytes.size());
        // last fromPluginBytes should be Duplicate
        PublishStreamResponse response = PublishStreamResponse.PROTOBUF.parse(fromPluginBytes.getLast());
        assertEquals(ACKNOWLEDGEMENT, response.response().kind());
        BlockAcknowledgement ack = response.acknowledgement().blockAck();
        BlockAcknowledgement expectedAck = new BlockAcknowledgement(1, null, true);
        assertEquals(expectedAck, ack, "Expected an ACK with flag of duplicate (blockAlreadyExists=true)");
    }

    @Test
    @DisplayName("When publisher receives a Future Block, it should send a status response with STREAM_ITEMS_BEHIND")
    void testFutureBlock() throws ParseException {
        // create sample block 1
        final BlockItem blockHeader1 = sampleBlockHeader(1);
        toPluginPipe.onNext(blockItemsToPublishStreamRequest(blockHeader1));
        // check the data was sent through to the block messaging facility
        assertEquals(1, blockMessaging.getSentBlockItems().size());
        // simulate verification success
        blockMessaging.sendBlockNotification(new BlockNotification(1, Type.BLOCK_VERIFIED, Bytes.wrap("hash1")));
        // simulate persistence success
        blockMessaging.sendBlockNotification(new BlockNotification(1, Type.BLOCK_PERSISTED, null));

        // create sample block 5
        final BlockItem blockHeader5 = sampleBlockHeader(5);
        toPluginPipe.onNext(blockItemsToPublishStreamRequest(blockHeader5));

        assertEquals(2, fromPluginBytes.size());
        PublishStreamResponse aheadResponse = PublishStreamResponse.PROTOBUF.parse(fromPluginBytes.getLast());

        // verify we get an endOfStream with status code STREAM_ITEMS_BEHIND
        assertEquals(STATUS, aheadResponse.response().kind());
        assertEquals(
                PublishStreamResponseCode.STREAM_ITEMS_BEHIND,
                aheadResponse.status().status());
        assertEquals(1, aheadResponse.status().blockNumber());
    }

    /*
        TODO not working yet , not sure why
        @Test
        void testPublisherSendsOnBlockVerificationFailedNotification() throws ParseException {
            assertEquals(0, fromPluginBytes.size());
            final Bytes hash = Bytes.fromHex("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
            blockMessaging.sendBlockNotification(new BlockNotification(100, Type.BLOCK_FAILED_VERIFICATION, hash));
            // check the data was sent through to the test block messaging facility
            assertEquals(1, blockMessaging.getSentBlockNotifications().size());
            // check if a response was sent to CN
            assertEquals(1, fromPluginBytes.size());
            PublishStreamResponse response = PublishStreamResponse.PROTOBUF.parse(fromPluginBytes.getFirst());
            System.out.println("response = " + response);


    //        assertEquals(ACKNOWLEDGEMENT, response.response().kind());
    //        final Acknowledgement ack = response.response().as();
    //        assertEquals(new BlockAcknowledgement(100,hash,true),
    //                ack.blockAck());
        }
         */

    /**
     * Helper method to convert a block items to a publish stream request bytes for sending to the plugin.
     * @param blockItems the block items to wrap
     * @return the bytes of PublishStreamRequest to the plugin
     */
    public static Bytes blockItemsToPublishStreamRequest(BlockItem... blockItems) {
        return PublishStreamRequest.PROTOBUF.toBytes(new PublishStreamRequest(
                new OneOf<>(RequestOneOfType.BLOCK_ITEMS, new BlockItemSet(List.of(blockItems)))));
    }
}
