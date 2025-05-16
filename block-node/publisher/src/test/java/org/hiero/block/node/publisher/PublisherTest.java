// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.publisher;

import static org.hiero.block.api.PublishStreamResponse.ResponseOneOfType.ACKNOWLEDGEMENT;
import static org.hiero.block.node.app.fixtures.TestUtils.enableDebugLogging;
import static org.hiero.block.node.app.fixtures.blocks.BlockItemUtils.toBlockItemJson;
import static org.hiero.block.node.app.fixtures.blocks.BlockItemUtils.toBlockItemsUnparsed;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.createNumberOfVerySimpleBlocks;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.sampleBlockHeader;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.sampleBlockProof;
import static org.hiero.block.node.app.fixtures.blocks.SimpleTestBlockItemBuilder.sampleRoundHeader;
import static org.hiero.block.node.publisher.PublisherServicePlugin.BlockStreamPublisherServiceMethod.publishBlockStream;
import static org.hiero.block.node.spi.BlockNodePlugin.UNKNOWN_BLOCK_NUMBER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.grpc.ServiceInterface.Method;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.hiero.block.api.BlockItemSet;
import org.hiero.block.api.PublishStreamRequest;
import org.hiero.block.api.PublishStreamRequest.RequestOneOfType;
import org.hiero.block.api.PublishStreamResponse;
import org.hiero.block.api.PublishStreamResponse.BlockAcknowledgement;
import org.hiero.block.api.PublishStreamResponse.EndOfStream.Code;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.plugintest.GrpcPluginTestBase;
import org.hiero.block.node.app.fixtures.plugintest.NoBlocksHistoricalBlockFacility;
import org.hiero.block.node.spi.blockmessaging.PersistedNotification;
import org.hiero.block.node.spi.blockmessaging.VerificationNotification;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for the PublisherServicePlugin. It mocks out the rest of the block node so we can simply test just this plugin.
 */
@SuppressWarnings({"FieldCanBeLocal", "MismatchedQueryAndUpdateOfCollection", "SameParameterValue", "DataFlowIssue"})
public class PublisherTest extends GrpcPluginTestBase<PublisherServicePlugin> {

    public PublisherTest() {
        start(new PublisherServicePlugin(), publishBlockStream, new NoBlocksHistoricalBlockFacility());
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
        assertEquals(0, blockMessaging.getSentVerificationNotifications().size());
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
        blockMessaging.sendBlockPersisted(new PersistedNotification(0, 0, 1));
        // check the data was sent through to the test block messaging facility
        assertEquals(1, blockMessaging.getSentPersistedNotifications().size());
        // check if a response was sent to CN
        assertEquals(1, fromPluginBytes.size());
        PublishStreamResponse response = PublishStreamResponse.PROTOBUF.parse(fromPluginBytes.getFirst());
        assertEquals(ACKNOWLEDGEMENT, response.response().kind());
        final BlockAcknowledgement ack = response.response().as();
        assertEquals(new BlockAcknowledgement(0, null, false), ack);
        // check second block
        blockMessaging.sendBlockPersisted(new PersistedNotification(1, 1, 1));
        PublishStreamResponse response1 = PublishStreamResponse.PROTOBUF.parse(fromPluginBytes.getLast());
        assertEquals(ACKNOWLEDGEMENT, response1.response().kind());
        final BlockAcknowledgement ack1 = response1.response().as();
        assertEquals(new BlockAcknowledgement(1, null, false), ack1);
    }

    @Test
    @DisplayName(
            "When publisher receives a Duplicated (already acked) block, it should send an ack with duplicate flag")
    void testPublisherDuplicateBlock() throws ParseException {
        // create sample block 0, the fact that it is 0 is important as it's the first block after -1, so we will get
        // ack
        final BlockItem[] block0 = createNumberOfVerySimpleBlocks(0, 0);
        toPluginPipe.onNext(blockItemsToPublishStreamRequest(block0));
        // check the data was sent through to the block messaging facility
        assertEquals(1, blockMessaging.getSentBlockItems().size());
        // simulate verification success
        blockMessaging.sendBlockVerification(new VerificationNotification(
                true, 0, Bytes.wrap("hash1"), new BlockUnparsed(toBlockItemsUnparsed(block0))));
        // simulate persistence success
        blockMessaging.sendBlockPersisted(new PersistedNotification(0, 0, 1));
        // should have got acknowledgement for first block
        assertEquals(1, fromPluginBytes.size());
        // re-attempt to send same block.
        toPluginPipe.onNext(blockItemsToPublishStreamRequest(block0));

        assertEquals(2, fromPluginBytes.size());
        // last fromPluginBytes should be Duplicate
        PublishStreamResponse response = PublishStreamResponse.PROTOBUF.parse(fromPluginBytes.getLast());
        assertEquals(ACKNOWLEDGEMENT, response.response().kind());
        BlockAcknowledgement ack = response.acknowledgement();
        BlockAcknowledgement expectedAck = new BlockAcknowledgement(0, null, true);
        assertEquals(expectedAck, ack, "Expected an ACK with flag of duplicate (blockAlreadyExists=true)");
    }

    @Test
    @DisplayName("When publisher receives a Future Block, it should send a status response with STREAM_ITEMS_BEHIND")
    void testFutureBlock() throws ParseException {
        // create sample block 0, the fact that it is 0 is important as it's the first block after -1, so we will get
        // ack
        final BlockItem[] block0 = createNumberOfVerySimpleBlocks(0, 0);
        toPluginPipe.onNext(blockItemsToPublishStreamRequest(block0));
        // check the data was sent through to the block messaging facility
        assertEquals(1, blockMessaging.getSentBlockItems().size());
        // simulate verification success
        blockMessaging.sendBlockVerification(new VerificationNotification(
                true, 0, Bytes.wrap("hash1"), new BlockUnparsed(toBlockItemsUnparsed(block0))));
        // simulate persistence success
        blockMessaging.sendBlockPersisted(new PersistedNotification(0, 0, 1));
        // should have got acknowledgement for first block
        assertEquals(1, fromPluginBytes.size());

        // create sample block 5
        final BlockItem[] block5 = createNumberOfVerySimpleBlocks(5, 5);
        toPluginPipe.onNext(blockItemsToPublishStreamRequest(block5));

        assertEquals(2, fromPluginBytes.size());
        PublishStreamResponse aheadResponse = PublishStreamResponse.PROTOBUF.parse(fromPluginBytes.getLast());

        // verify we get an endOfStream with status code BEHIND
        assertEquals(Code.BEHIND, aheadResponse.endStream().status());
        assertEquals(0, aheadResponse.endStream().blockNumber());
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
